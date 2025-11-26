from airflow import DAG
from airflow.operators.bash import BashOperator
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
    description='DAG to ingest music market share data from kworb.net',
    schedule_interval='0 9 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['music', 'ingestion'],
) as dag:
    
    scrape_kworb_task = BashOperator(
        task_id='scrape_kworb_data',
        bash_command='python /opt/airflow/scripts/scrape_kworb.py'
    )

    scrape_kworb_task