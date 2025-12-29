from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'music_market_share_ingest',
    default_args=default_args,
    description='Daily pipeline: scrape charts, trigger enrichment pipeline, build dbt marts',
    schedule_interval='0 9 * * *',
    start_date=datetime(2025, 12, 18),
    catchup=False,
    max_active_runs=1,
    tags=['music', 'ingestion', 'dbt'],
) as dag:

    # 1. Scrape Kworb charts data
    scrape_kworb_task = BashOperator(
        task_id='scrape_kworb_data',
        bash_command='python /opt/airflow/scripts/scrape_kworb.py'
    )

    # 2. Trigger the enrichment pipeline DAG
    trigger_pipeline = TriggerDagRunOperator(
        task_id='trigger_market_share_pipeline',
        trigger_dag_id='music_market_share_pipeline',
        logical_date='{{ logical_date }}',
        reset_dag_run=True,
        wait_for_completion=False,
    )

    # 3. Wait for the enrichment pipeline to complete
    wait_pipeline = ExternalTaskSensor(
        task_id='wait_for_market_share_pipeline',
        external_dag_id='music_market_share_pipeline',
        external_task_id='pipeline_complete',
        allowed_states=['success'],
        failed_states=['failed', 'upstream_failed'],
        mode='reschedule',
        poke_interval=30,
        timeout=60 * 60,  # 1 hour timeout
    )

    # 4. Build dbt models after enrichment completes
    build_dbt_models_task = BashOperator(
        task_id='build_dbt_models',
        bash_command=(
            'dbt deps '
            '--project-dir /opt/airflow/dbt_project '
            '--profiles-dir /opt/airflow/dbt_project '
            '&& '
            'dbt run '
            '--project-dir /opt/airflow/dbt_project '
            '--profiles-dir /opt/airflow/dbt_project '
            '--select +fact_market_share'
        ),
    )

    scrape_kworb_task >> trigger_pipeline >> wait_pipeline >> build_dbt_models_task