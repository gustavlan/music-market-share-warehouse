from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'setup_musicbrainz_data',
    default_args={'owner': 'airflow'},
    description='One-time load of MusicBrainz JSON dumps',
    schedule_interval=None,  # trigger manually
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['setup', 'musicbrainz'],
) as dag:

    load_labels = BashOperator(
        task_id='load_labels_to_duckdb',
        bash_command='python /opt/airflow/scripts/load_musicbrainz.py'
    )