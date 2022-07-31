# Data Pipeline using Apache Airflow, DBT and Snowflake 

## Technologies
* Python 
* AWS S3 
* Apache Airflow - Open Source
* Snowflake - Enterprise
* DBT (Data Build Tool) - Open Source 
* PostgreSQL 

## Operating System 
* Ubuntu 20.04

## Description 
This project simulates a data pipeline. Data is crawled from an online shop's website and stored in AWS S3. With the help of Airflow, data is loaded into Snowflake warehouse and transformed using DBT then stored in a database. 


## Steps
1. Setup the crawler to crawl price data from an online shop. I used jumia's website(https://www.jumia.com.gh) for this project. Run data_transform.py file to convert the json files into CSV
2. Create a Snowflake account if you do not have one. A trial period is available and should enable you complete the project
3. Run the queries in the snowflake_queries.sql script to create a custom role, user, warehouse and database for this project 
4. [Optional] Install PostgreSQL on your development machine which I used to run Airflow. Airflow comes with default SQLite but can also be used on Redshift, Snowflake, PostgreSQL. 
5. Setup the Airflow home `export AIRFLOW_HOME=~/airflow`
5. Setup a Python virtual environment (Create and activate)
6. Install the required packages by running `pip install -r requirements.txt`. The packages are in the requirements.txt file 
7. Run `airflow info` to get the information on the airflow arguments. This will print to the console the location of airflow.cfg which is the config file for Airflow. In the config file, edit the parameters to match your DAGs folder and postgresql connection
8. Initialise the Airflow db with `airflow db init`
9. Create an airflow user for the project `airflow users create --username admin --password admin --role Admin --firstname admin --lastname admin --email admin`
10. Run the airflow scheduler with `airflow scheduler`
11. In a new tab in the terminal with the virtul environment activated, run the Airflow webserver with `airflow webserver`
12. From the `airflow.cfg` file, make sure your `dag_folder` exists because your DAGs will run from there. If you are just starting and will like to see the examples and run them, make sure `load_examples` is set to True in `ariflow.cfg` otherwise, it can be set to False so you can have only the dags you are about to create.
13. 



