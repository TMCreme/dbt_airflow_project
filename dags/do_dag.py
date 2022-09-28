from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
# from do_read import read_from_s3
from do_read import ReadFromS3


# Define the default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["admin"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    "do_pipeline_test",
    default_args=default_args,
    description="Testing the scenarios for DO Pipeline",
    schedule_interval=timedelta(days=2),
)


# Using S3Hook to download the file
# File will be saved in a temporary dir and value returned
def downloadfile():
    hook = S3Hook('aws_s3_conn')
    return hook.download_file(
        Variable.get("do_s3_file"),
        bucket_name=Variable.get("do_bucket_name")
    )


# Running the download function in a PythonOperator
# The returned value from the funtion, the filename will be in xcom
download_file = PythonOperator(
    task_id="download_s3_file",
    python_callable=downloadfile,
    dag=dag,
)


# Function to get the filename from xcom so the file can be read
# The value is saved in the airflow Variables
def get_tmp_file_name(**context):
    filename = context["task_instance"].xcom_pull(task_ids="download_s3_file")
    Variable.set("do_downloaded_file", filename)


# Calling the above function in a PythonOperator
save_tmp_filename = PythonOperator(
    task_id="set_tmp_filename",
    python_callable=get_tmp_file_name,
    dag=dag
)


local_file_sensor = FileSensor(
    task_id="sensing_local_file",
    filepath=Variable.get("do_downloaded_file"),
    dag=dag
)

process_file = PythonOperator(
    task_id="process_s3_file",
    python_callable=ReadFromS3().save_to_csv,
    op_args=[
        Variable.get("do_downloaded_file"),
        Variable.get("do_new_file")
    ],
    dag=dag
)


def pull_function(**context):
    key_list = context["task_instance"].xcom_pull(task_ids="process_s3_file")
    for item, val in key_list.items():
        Variable.set(item, val)
    return key_list


save_val_in_variables = PythonOperator(
    task_id="setting_variables",
    python_callable=pull_function,
    dag=dag
)

creating_table = PostgresOperator(
    task_id="creating_table_with_column",
    postgres_conn_id="postgres_default",
    sql=Variable.get("table_create_sql"),
    dag=dag,
)


def copy_into_postgres():
    postgre_hook = PostgresHook(postgres_conn_id="postgres_default")
    postgre_hook.copy_expert(
        """COPY airflow.public.billbox_accounts FROM stdin WITH CSV HEADER
            DELIMITER as ',' """,
        Variable.get("do_download_file"),
    )


copy_local_to_postgres = PythonOperator(
    task_id="copy_from_s3_into_postgres",
    python_callable=copy_into_postgres,
    dag=dag
)

dbt_operator = BashOperator(
    task_id="dbt_task_in_airflow",
    bash_command="cd /opt/airflow/airflowproj && dbt deps && dbt run --profiles-dir .",
    dag=dag,
)

(
    download_file
    >> save_tmp_filename
    >> local_file_sensor
    >> process_file
    >> save_val_in_variables
    >> creating_table
    >> copy_local_to_postgres
    >> dbt_operator
)
