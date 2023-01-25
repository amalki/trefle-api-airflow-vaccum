import datetime
import pendulum
import requests
import logging

from airflow import DAG

from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

from src.fetch_api_data import fetch_data_to_local
from src.upload_to_s3 import multiple_upload_to_s3


with DAG(
    dag_id="ingest_plants_data_to_snowflake_dag",
    schedule_interval="@once",
    start_date=pendulum.datetime(2021, 1, 24, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    render_template_as_native_obj=True,  # to render native python objects with jinja template
) as dag:

    # check api health
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
        python_callable=fetch_data_to_local,
        dag=dag,
    )

    # upload all written files to s3
    task_upload_local_csv_files_to_s3 = PythonOperator(
        task_id="upload_local_csv_files_to_s3",
        python_callable=multiple_upload_to_s3,
        dag=dag,
    )

    # create or replace stage
    task_create_s3_snowflake_stage = SnowflakeOperator(
        task_id="create_s3_snowflake_stage",
        sql="./sql/create_stage_s3.sql",
        snowflake_conn_id="snowflake_default",
        params={"stage": "my_dynamic_stage"}, # for dynamic queries
        dag=dag,
    )

    # create or replace table in snowflake
    task_create_snowflake_table = SnowflakeOperator(
        task_id="create_snowflake_table",
        sql="./sql/create_table_plants.sql",
        snowflake_conn_id="snowflake_default",
        params={"stage": "my_dynamic_stage"},
        dag=dag,
    )

    # load data to snowflake table
    task_load_to_snowflake_table = S3ToSnowflakeOperator(
        task_id="load_to_snowflake_table",
        snowflake_conn_id="snowflake_default",
        s3_keys="{{ task_instance.xcom_pull(task_ids='upload_local_csv_files_to_s3', key='s3_file_paths') }}",
        table="plants",
        stage="stage_trefle_s3",
        file_format="(type = 'CSV',field_delimiter = '|')",
        dag=dag,
    )

    (
        task_http_sensor_check
        >> task_fetch_plants_data_to_local
        >> task_upload_local_csv_files_to_s3
        >> task_create_s3_snowflake_stage
        >> task_create_snowflake_table
        >> task_load_to_snowflake_table
    )
