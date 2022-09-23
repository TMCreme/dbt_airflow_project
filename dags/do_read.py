import pandas as pd
import logging
import boto3
from airflow.models import Variable


class ReadFromS3:
    def __init__(self) -> None:
        logging.basicConfig(
            filename="custom_read.log",
            filemode='a',
            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
            datefmt='%H:%M:%S',
            level=logging.DEBUG
        )
        self.logger = logging.getLogger("initialization")
        self.access_key = Variable.get("do_aws_credntials_access_token")
        self.secret_key = Variable.get("do_aws_credntials_secret_token")
        self.logger.debug("Getting the S3 connection")
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )
        self.logger.debug("Connected to S3 {}".format(self.s3))

    # This function reads the file from s3
    def read_file_object(self, s3_file: str) -> pd.DataFrame:
        # call the s3 connection function above
        s3 = self.s3
        self.logger = logging.getLogger("read_file_object")
        self.logger.debug("Getting bucket name from Airflow variables")
        # Retrieving the bucket name from Airflow variables
        bucket_name = Variable.get("do_bucket_name")
        self.logger.debug("Reading the S3 object")
        # Reading the s3 object
        response = s3.get_object(
            Bucket=bucket_name,
            Key=s3_file)
        self.logger.debug("Response object: {}".format(response))
        # Reading the body with data into pandas df
        df = pd.read_json(response.get("Body"), lines=True)
        self.logger.debug(
            "Read data from into dataframe: {}".format(df.head())
            )
        return df

    # Cleaning the file to have strings
    def clean_up_file(self, s3_file: str, local_file: str):
        # calling the read function
        df = self.read_file_object(s3_file=s3_file)
        self.logger = logging.getLogger("clean_up_file")
        self.logger.debug("Cleaning up the dataframe objectId")
        # stripping off the objectID in mongodb object to
        # make it a string
        df["_id"] = df["_id"].astype(str).str.lstrip("{'$oid': '")
        df["_id"] = df["_id"].astype(str).str.strip("'}")

        # {'$date': '2015-05-07T16:08:00.201Z'}
        # Replacing the empty date fields with a None because
        # transforming nan in SQL was a problem
        self.logger.debug("Cleaning up the dataframe date")
        df["dateModified"] = df["dateModified"].fillna("")
        df["dateCreated"] = df["dateCreated"].fillna("")

        # Stripping the date objects to have only the date string
        # Mongodb stores the datetime object in a unique format
        df["dateCreated"] = (
            df["dateCreated"].astype(str).str.lstrip("{'$date': '")
        )
        df["dateCreated"] = df[
            "dateCreated"
            ].astype(str).str.strip("'}")

        df["dateModified"] = (
            df["dateModified"].astype(str).str.lstrip("{'$date': '")
        )
        df["dateModified"] = df[
            "dateModified"
            ].astype(str).str.strip("'}")
        # calling the save to csv function to convert df to csv
        self.logger.debug("Cleaning up completed successfully")
        self.save_to_csv(df=df, local_file=local_file)
        # return

    # Converting df to CSV and setting the variables in airflow
    def save_to_csv(self, df: pd.DataFrame, local_file: str) -> None:
        self.logger = logging.getLogger("save_to_csv")
        # Converting the dataframe to csv
        self.logger.debug("Converting the datafrome to CSV")
        df.to_csv(local_file, index=False)
        # Save CSV filename into a variable
        Variable.set("do_download_file", local_file)
        self.logger.debug("CSV Filename: {}".format(local_file))
        # Read the csv headers into a list
        self.logger.debug("Read the CSV headers into a list")
        df_header = pd.read_csv(local_file, nrows=0).columns.tolist()
        self.logger.debug("Header List: {}".format(df_header))
        # Create an SQL CREATE statement as a string variable
        # and save in Airflow variables
        self.logger.debug(
            """Create an SQL CREATE TABLE Statement from using the
             headers of the dataframe"""
        )
        df_header_edit = [
            str(item) + " character varying(500)" for item in df_header
            ]
        self.logger.debug(
            "SQL Create Statement Columns: {}".format(df_header_edit)
            )
        # Converting the list of SQL columns into a string
        self.logger.debug("Convert the list of columns into a string")
        creation_script = "".join(str(x) + "," for x in df_header_edit)
        self.logger.debug("CREATE TABLE Columns: {}".format(creation_script))
        # Add the CREATE keyword and save it in an Airflow var
        self.logger.debug("Add the CREATE keyword and save in a var")
        Variable.set(
            "table_create_sql",
            """CREATE TABLE IF NOT EXISTS
            airflow.public.billbox_accounts ( {} )""".format(
                creation_script[:-1]
            ),
        )
        self.logger.debug("File handling complete. ")
        return None
