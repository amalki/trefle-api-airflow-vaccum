import datetime
import pendulum

import requests

from airflow import DAG

from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

from src.fetch_api_data import fetch_data_to_local_old

with DAG(
    dag_id="ingest_plants_data_to_snowflake_dag_old",
    schedule_interval="@once",
    start_date=pendulum.datetime(2021, 1, 23, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:

    # check api health and get total number of pages
    task_http_sensor_check = HttpSensor(
        task_id="http_sensor_check",
        http_conn_id="api_default",
        endpoint="plants",
        request_params={"token": "{{ var.value.get('trefle_api_token', 'fallback') }}"},
        method="GET",
        poke_interval=5,  # poke every 5s
        timeout=30,  # 30s then it stops poking
        dag=dag,
    )

    # fetch all data to local
    task_fetch_plants_data_to_local = PythonOperator(
        task_id="fetch_plants_data_to_local",
        python_callable=fetch_data_to_local_old,
        op_kwargs={"param_1": "one"},
        dag=dag,
    )

    # upload data from local to s3
    task_upload_local_csv_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_local_csv_to_s3",
        filename="/opt/data/plants.csv",
        dest_key="plants.csv",
        dest_bucket="trefle-bucket",
        replace=True,
        dag=dag,
    )

    # create or replace stage
    task_create_s3_snowflake_stage = SnowflakeOperator(
        task_id="create_s3_snowflake_stage",
        sql="./sql/create_stage.sql",
        snowflake_conn_id="snowflake_default",
        params={"stage": "my_dynamic_stage"},
        dag=dag,
    )

    # create or replace table in snowflake
    task_create_snowflake_table = SnowflakeOperator(
        task_id="create_snowflake_table",
        sql="./sql/create_table.sql",
        snowflake_conn_id="snowflake_default",
        params={"stage": "my_dynamic_stage"},
        dag=dag,
    )

    # load data to snowflake table from s3
    task_load_to_snowflake_table = S3ToSnowflakeOperator(
        task_id="load_to_snowflake_table",
        snowflake_conn_id="snowflake_default",
        s3_keys=["plants.csv"],
        table="plants",
        stage="stage_trefle_s3",
        file_format="(type = 'CSV',field_delimiter = '|')",
        dag=dag,
    )

    (
        task_http_sensor_check
        >> task_fetch_plants_data_to_local
        >> task_upload_local_csv_to_s3
        >> task_create_s3_snowflake_stage
        >> task_create_snowflake_table
        >> task_load_to_snowflake_table
    )
