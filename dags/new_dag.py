import logging 
from datetime import timedelta, datetime
import snowflake
from airflow import DAG 
from airflow.utils.dates import days_ago 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator



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

# This is a simple dag definition for the purpose of this mini project 
dag = DAG(
    "s3_dbt_snowflake",
    default_args=default_args,
    description="Testing the scenarios from MAC",
    schedule_interval=timedelta(days=2)
)

# This task gets the CSV files in an AWS S3 bucket and stores them in a returned value in the xcom object of Airflow
list_keys = S3ListOperator(
    task_id = "list_files_in_s3_bucket",
    aws_conn_id="s3_conn",
    bucket="transforme-products",
    dag=dag
)

# An empty list that will be the store for the s3 files keys for the list_keys task above. 
# This is global so it can be referenced later 
key_list = []

# This gets the values of a returned object from an airflow task generally 
# So I used it to get the s3 files keys in order to store them in the variable defined above so I can reference it in other functions
def pull_function(**context):
    key_list = context["task_instance"].xcom_pull(task_ids="list_files_in_s3_bucket")
    return key_list

# This is a python operator task that I will use to call the function getting the returned values above.
# The function needs to be in an task to run in the Airflow orchestration 
get_keys = PythonOperator(
    task_id="get_s3_keys",
    python_callable=pull_function,
    dag=dag
)

# The S3ToSnowflakeOperator takes data from a stage into a table by referencing named s3_keys
# Because the s3_keys must contain the named files, I used the key_list that will store the keys from the List Operator 
copy_into_table = S3ToSnowflakeOperator(
    snowflake_conn_id="snowflake_conn",
    task_id="s3_to_snowflake",
    s3_keys=key_list,
    stage="staging",
    table="jumia_products",
    file_format="(type='csv')",
    dag=dag
)


dbt_operator = BashOperator(
    task_id="dbt_task_in_airflow",
    bash_command="cd /home/tonny/dteng/airflowproj && dbt run",
    dag=dag
)




list_keys >> get_keys >> copy_into_table >> dbt_operator














