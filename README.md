# Data Pipeline using Apache Airflow, DBT and Snowflake 

## Technologies
* Python 
* Pandas 
* AWS S3 
* Apache Airflow - Open Source
* DBT (Data Build Tool) - Open Source 
* PostgreSQL 

## Operating System 
* Ubuntu 20.04

## Description 
This project simulates a data pipeline. The data used for starting the project has changed. It started with a crawled data from an online shop but I needed more data types to test various scenarios so got some other data to use. Data from the AWS S3 is extracted with the help pandas in python into a postgres warehouse. DBT does the transformation on the data and loads it into it's appropriate table.  


## Steps
1. The crawler_dag.py is to crawl price data from an online shop. I used jumia's website(https://www.jumia.com.gh) for this project. The data is stored in CSV files and uploaded to S3. This is triggered by an airflow dag.
2. Install PostgreSQL on your development machine which I used to run Airflow. Airflow comes with default SQLite but can also be used on Redshift, Snowflake, PostgreSQL. I used Postgresql as my main database. 
3. Clone the feature branch of the project.
4. Run `docker compose up --build` from the root
5. When it starts, visit the `localhost:8080` in the browser and create all the necessary connection, file(path), AWS S3, PostgreSQL. 
6. Import the `variables.json` into the Airflow variables
7. Run the dag `do_pipeline_test`. i.e. Visit the Airflow UI and run the dag that has been created. This can be scheduled to run anytime of the day or how frequent you want. That is the point of Airflow. 

## Docker Compose
* Deployment with Docker
The project will be deployed to a Production environment with docker. 

sudo apt-get install ca-certificates curl gnupg lsb-release