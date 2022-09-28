from typing import TextIO
import pandas as pd
import logging


# I have removed all airflow operations from here
# Any airflow operation will be handled in the dag
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
        # self.clean_up_file()

    # Cleaning the file to have strings
    def clean_up_file(self, s3_file: TextIO) -> pd.DataFrame:
        # Read the file data into a dataframe
        self.df = pd.read_json(s3_file, lines=True)
        self.logger = logging.getLogger("clean_up_file")
        self.logger.debug("Cleaning up the dataframe objectId")
        # stripping off the objectID in mongodb object to
        # make it a string
        self.df["_id"] = self.df["_id"].astype(str).str.lstrip("{'$oid': '")
        self.df["_id"] = self.df["_id"].astype(str).str.strip("'}")

        # {'$date': '2015-05-07T16:08:00.201Z'}
        # Replacing the empty date fields with a None because
        # transforming nan in SQL was a problem
        self.logger.debug("Cleaning up the dataframe date")
        self.df["dateModified"] = self.df["dateModified"].fillna("")
        self.df["dateCreated"] = self.df["dateCreated"].fillna("")

        # Stripping the date objects to have only the date string
        # Mongodb stores the datetime object in a unique format
        self.df["dateCreated"] = (
            self.df["dateCreated"].astype(str).str.lstrip("{'$date': '")
        )
        self.df["dateCreated"] = self.df[
            "dateCreated"
            ].astype(str).str.strip("'}")

        self.df["dateModified"] = (
            self.df["dateModified"].astype(str).str.lstrip("{'$date': '")
        )
        self.df["dateModified"] = self.df[
            "dateModified"
            ].astype(str).str.strip("'}")
        # calling the save to csv function to convert df to csv
        self.logger.debug("Cleaning up completed successfully")
        return self.df

    # Converting df to CSV and setting the variables in airflow
    def save_to_csv(self, s3_file: TextIO, local_file: TextIO) -> dict:
        self.clean_up_file(s3_file)
        print("Initializing the save to csv")
        self.logger = logging.getLogger("save_to_csv")
        # Converting the dataframe to csv
        self.logger.debug("Converting the datafrome to CSV")
        self.df.to_csv(local_file, index=False)
        print("Saved to csv")
        # Save CSV filename into a variable
        data_dict = {}
        data_dict["do_download_file"] = local_file
        # Variable.set("do_download_file", local_file)
        self.logger.debug("CSV Filename: {}".format(local_file))
        # Read the csv headers into a list
        self.logger.debug("Read the CSV headers into a list")
        print("read from csv")
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
        # Add the CREATE keyword and save it in a dictonary
        self.logger.debug("Add the CREATE keyword and save in a var")
        data_dict["table_create_sql"] = """CREATE TABLE IF NOT EXISTS
            airflow.public.billbox_accounts ( {} )""".format(
                creation_script[:-1]
            )
        self.logger.debug("File handling complete. ")
        # Return a data so the operator can access and save as variable
        # Data includes CREATE TABLE STATEMENT and local filename
        return data_dict
