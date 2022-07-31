USE SCHEMA DBT_AIRFLOW_DB.public;
CREATE OR REPLACE STAGE staging
   url='s3://<bucket_name>'
   credentials=(
     aws_key_id='<AWS_ACCESS_KEY>'
     aws_secret_key='<AWS_SECRET_ACCESS_KEY>')
   file_format = (type = csv);