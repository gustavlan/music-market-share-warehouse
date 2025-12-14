{{ config(materialized='view') }}

select
  *
from read_parquet(
  '{{ env_var("KWORB_PARQUET_GLOB", "/opt/airflow/data/raw/kworb/*.parquet") }}'
);