#!/bin/bash

echo "Starting Webserver"
airflow db init 
airflow db upgrade
airflow users create \
    --username "admin" \
    --firstname "Airflow" \
    --lastname "Admin" \
    --email "airflowadmin@example.com" \
    --role "Admin" \
    --password "admin" ||
airflow create_user \
    --username "admin" \
    --firstname "Airflow" \
    --lastname "Admin" \
    --email "airflowadmin@example.com" \
    --role "Admin" \
    --password "admin" 
airflow scheduler & airflow webserver && fg
exec "$@"