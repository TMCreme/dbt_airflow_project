import logging 
from datetime import timedelta, datetime
from airflow import DAG 
from airflow.utils.dates import days_ago 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

# from airflow.providers.post

from crawler_dag import category_data


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
    "test_file_sensor",
    default_args=default_args,
    description="Testing the scenarios for File Sensor",
    schedule_interval=timedelta(days=2)
)

crawling_data = PythonOperator(
    task_id="crawl_data_from_online_shop",
    python_callable=category_data,
    dag=dag,
)


copy_to_s3 = LocalFilesystemToS3Operator(
    task_id="copy_file_to_s3",
    filename=Variable.get("local_file_full_path"),
    aws_conn_id="s3_conn",
    dest_bucket=Variable.get("destination_bucket_name"),
    dest_key=Variable.get("latest_uploaded_file"),
    dag=dag
)

get_files = S3KeySensor(
    task_id="getting_files_by_sensor",
    aws_conn_id="s3_conn",
    bucket_name=Variable.get("destination_bucket_name"),
    bucket_key=Variable.get("latest_uploaded_file"),
    dag=dag,
)

def copy_into_postgres(bucket_name, filename):
    postgre_hook = PostgresHook(postgres_conn_id="postgres_default")
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    postgre_hook.copy_expert(
        """COPY dbt_airflow_db.public.jumia_products FROM stdin WITH CSV HEADER
            DELIMITER as ',' """,
            "transforms/"+Variable.get("latest_uploaded_file")
            # s3_hook.get_key(bucket_name=bucket_name,key=filename)
    )

copy_s3_to_postgres = PythonOperator(
    task_id="copy_from_s3_into_postgres",
    python_callable=copy_into_postgres,
    op_args=[Variable.get("destination_bucket_name"),Variable.get("latest_uploaded_file")],
    dag=dag
)




dbt_operator = BashOperator(
    task_id="dbt_task_in_airflow",
    bash_command="cd /home/tonny/dteng/airflowproj && dbt run",
    dag=dag
)

# postgres_conn = PostgresHook(
#     conn_name_attr="postgres_default",
#     copy_expert = ("""COPY dbt_airflow_db.public.jumia_products  FROM stdin WITH CSV HEADER
#                         DELIMITER as ',' """, 
#                         's3://jumia_products/'+Variable.get("latest_uploaded_file"))
# )

crawling_data >> copy_to_s3 >> get_files >> copy_s3_to_postgres >> dbt_operator






