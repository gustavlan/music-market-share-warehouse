from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import duckdb
import os

# Configuration
DB_PATH = '/opt/airflow/data/music_warehouse.duckdb'
SCRIPTS_DIR = '/opt/airflow/scripts'


def load_kworb_to_duckdb():
    """
    Loads the latest Kworb parquet files into a staging object
    so the enrichment script can query them.
    """
    con = duckdb.connect(DB_PATH)
    # Ensure a consistent object type for downstream steps.
    # dbt or previous runs may have created stg_combined_charts as a VIEW.
    existing_type = con.execute(
        """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = 'stg_combined_charts'
        """
    ).fetchone()

    if existing_type is not None and existing_type[0] != 'VIEW':
        con.execute("DROP TABLE stg_combined_charts")
    # Create the view expected by enrich_metadata.py (and compatible with dbt defaults)
    con.execute("""
        CREATE OR REPLACE VIEW stg_combined_charts AS 
        SELECT * FROM read_parquet('/opt/airflow/data/raw/kworb/*.parquet')
    """)
    con.close()


def check_if_enrichment_needed():
    """
    Returns True if there are tracks in staging that are missing from the metadata table.
    Also ensures dim_track_metadata table exists (critical for dbt on fresh DB).
    """
    con = duckdb.connect(DB_PATH)
    
    # Ensure metadata table exists (so dbt doesn't fail on fresh warehouse)
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_track_metadata (
            track_name VARCHAR,
            artist_name VARCHAR,
            spotify_label VARCHAR,
            spotify_track_id VARCHAR,
            updated_at TIMESTAMP
        );
    """)
    
    # Logic mirrored from enrich_metadata.py
    query = """
        SELECT COUNT(*) 
        FROM stg_combined_charts c
        LEFT JOIN dim_track_metadata m
            ON c.track_name = m.track_name AND c.artist_name = m.artist_name
        WHERE m.spotify_label IS NULL
    """
    try:
        count = con.sql(query).fetchone()[0]
    except Exception as e:
        print(f"Error checking enrichment need: {e}")
        count = 0
    finally:
        con.close()
    
    print(f"Found {count} tracks requiring enrichment.")
    return count > 0


def choose_enrichment_path():
    return 'enrich_metadata' if check_if_enrichment_needed() else 'skip_enrichment'


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'music_market_share_pipeline',
    default_args=default_args,
    description='Load Kworb data and enrich with Spotify metadata (triggered by ingest DAG)',
    schedule_interval=None,  # Trigger-only: called by music_market_share_ingest
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['music', 'pipeline', 'spotify'],
) as dag:

    # 1. Load Parquet to DuckDB (Required for enrichment script to see the data)
    load_data = PythonOperator(
        task_id='load_kworb_to_duckdb',
        python_callable=load_kworb_to_duckdb
    )

    # 2. Decide whether to run expensive API calls.
    # Also ensures dim_track_metadata table exists for dbt.
    choose_enrichment = BranchPythonOperator(
        task_id='choose_enrichment_path',
        python_callable=choose_enrichment_path
    )

    skip_enrichment = EmptyOperator(
        task_id='skip_enrichment'
    )

    # 3. Run Spotify Enrichment if check_enrichment returns True
    enrich_metadata = BashOperator(
        task_id='enrich_metadata',
        bash_command=f'python {SCRIPTS_DIR}/enrich_metadata.py',
        env={
            'DATA_DIR': '/opt/airflow/data',
            'SPOTIPY_CLIENT_ID': os.getenv('SPOTIPY_CLIENT_ID'),
            'SPOTIPY_CLIENT_SECRET': os.getenv('SPOTIPY_CLIENT_SECRET')
        }
    )

    # 4. Final task for ExternalTaskSensor to wait on
    # Runs whether enrichment happened or was skipped
    pipeline_complete = EmptyOperator(
        task_id='pipeline_complete',
        trigger_rule='none_failed_min_one_success'
    )

    # Dependencies: load -> choose -> (enrich OR skip) -> complete
    load_data >> choose_enrichment
    choose_enrichment >> enrich_metadata >> pipeline_complete
    choose_enrichment >> skip_enrichment >> pipeline_complete
