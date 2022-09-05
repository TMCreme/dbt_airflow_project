import logging 
from datetime import timedelta, datetime
from airflow import DAG 
from airflow.utils.dates import days_ago 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

# from airflow.providers.post

from do_read import read_from_s3


# Define the default arguments for the DAG 
default_args = {
    "owner" : "airflow",
    "depends_on_past" : False,
    "start_date" : days_ago(2),
    "email" : ["admin"],
    "email_on_failure" : False,
    "email_on_retry" : False,
    "retries" : 0,
    "retry_delay" : timedelta(minutes=5)
}


dag = DAG(
    "do_pipeline_test",
    default_args=default_args,
    description="Testing the scenarios for DO Pipeline",
    schedule_interval=timedelta(days=2)
)


download_file = PythonOperator(
    task_id="download_s3_file",
    python_callable=read_from_s3,
    op_args=["/tmp/billbox_account_20220713.csv","backups/billbox_account_20220713.json"],
    dag=dag
)

local_file_sensor = FileSensor(
    task_id="sensing_local_file",
    filepath=Variable.get("do_download_file"),
    dag=dag
)

# create_script_file_sensor = FileSensor(
#     task_id="sensing_local_file_sql",
#     filepath="/create_script.sql",
#     dag=dag
# )

creating_table = PostgresOperator(
    task_id = "creating_table_with_column",
    postgres_conn_id="postgres_default",
    sql=Variable.get("table_create_sql"),
    dag=dag
)

def copy_into_postgres():
    postgre_hook = PostgresHook(postgres_conn_id="postgres_default")
    postgre_hook.copy_expert(
        """COPY do_billbox.public.billbox_accounts FROM stdin WITH CSV HEADER
            DELIMITER as ',' """,
            Variable.get("do_download_file")
    )

copy_local_to_postgres = PythonOperator(
    task_id="copy_from_s3_into_postgres",
    python_callable=copy_into_postgres,
    dag=dag
)

download_file >> local_file_sensor >> creating_table >> copy_local_to_postgres
