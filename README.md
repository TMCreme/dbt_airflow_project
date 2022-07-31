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
6. Setup a Python virtual environment (Create and activate)
7. Install the required packages by running `pip install -r requirements.txt`. The packages are in the requirements.txt file 
8. Run `airflow info` to get the information on the airflow arguments. This will print to the console the location of airflow.cfg which is the config file for Airflow. In the config file, edit the parameters to match your DAGs folder and postgresql connection
9. Initialise the Airflow db with `airflow db init`
10. Create an airflow user for the project `airflow users create --username admin --password admin --role Admin --firstname admin --lastname admin --email admin`
11. Run the airflow scheduler with `airflow scheduler`
12. In a new tab in the terminal with the virtul environment activated, run the Airflow webserver with `airflow webserver`
13. From the `airflow.cfg` file, make sure your `dag_folder` exists because your DAGs will run from there. If you are just starting and will like to see the examples and run them, make sure `load_examples` is set to True in `ariflow.cfg` otherwise, it can be set to False so you can have only the dags you are about to create.
14. On the snowflake console, let's create an external stage that links to the AWS S3 bucket and can read files for us by running the query in `staging.sql` 
15. Confirm the files are loaded successfully by running `LIST @staging`
16. Since the airflow webserver is still active, visit the airflow UI in the browser on localhost:8080 (if something else is running on port 8080, you can confirm what port airflow is running on in the terminal in which you run the airflow webserver and use that)
17. Login with the credentials of the user creted earlier (admin:admin) and create a connection to Snowflake by going to `Admin -> Connections` and add a new connection. Fill out the details and select the connection type to be Snowflake. In case Snowflake is not in the list. Stop the webserver and check if `apache-airflow-providers-snowflake` is installed in your environment and install it. Then run the webserver again and Snowflake should appear in the list. 
18. Add an Amazon S3 connection as well to connect to the S3 bucket by setting the connection ID and selecting the connection type as Amazon S3 then in the extra field add a json object in this format
{"aws_access_key_id": "XXXXXXXXXXXXX", "aws_secret_access_key": "XXXXXXXXXXXX"}
19. We are now connected to both S3 and Snowflake
20. We should also have dbt installed from the requirements file hence we can start the dbt project too now but we need a table in the table that we can load the data from the files into. 
21. Go to the Snowflake console and create a table in the database. Note that the data in the csv files comes as a string/varchar no matter the format in which you saved data in the file, hence the columns should all be varchar types like below. 
    `CREATE TABLE <table_name> (date varchar(100), category varchar(100), name varchar(350),price varchar(100), percentage varchar(100), currency varchar(50)`
22. Kindly take note of the table which was just created because it will be used as the table in the S3ToSnowFlakeOperator task. 
23. Create the dbt project with the command `dbt init airflowproj`. dbt init is the command and airflowproj is my project name. This will do a couple of things, like create your dbt profiles in your user's home like `~/.dbt/profiles.yml` and create a project folder in the location from which the dbt init was run. 
24. Check the dbt_project.yml file to ensure the values are accurate. I added a new directory as created under models to reference a table. The default is a view
25. The skeleton project comes with some predefined code. DBT is for the transformation in ETL/ELT so we will create a dag that will contain task to run the dbt transformation. 
26. create a directory under the models folder in the dbt project and create the schema.yml file, product_category.sql and product_detail.sql files. 
27. Create a dag like the new_dag.py file in this project and check the values defined in  to match. 
28. A brif about the tasks in the dag. 
    * list_keys - uses the S3ListOperator to retrieve the keys (filenames) from AWS S3. 
    * pull_function - retrieves the returned value from list_keys which airflow stores 
    * get_keys - saves the keys in a list accessible to other functions in the dag 
    * copy_into_table reads data from the files in the snowflake stage and saves it in the table we created above which should be referenced in the function
    * dbt_operator - as the name suggests, runs the dbt task to handle transformation of data types and saves in a table/view in the database.
28. Visit the Airflow UI and run the dag that has been created. This can be scheduled to run anytime of the day or how frequent you want. That is the point of Airflow. 



## Next
* Deployment with Docker
The project will be deployed to a Production environment with docker. 

