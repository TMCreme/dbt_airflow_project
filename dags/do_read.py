import pandas as pd
import boto3
from airflow.models import Variable


def read_from_s3(local_file, s3_file):
    access_key = Variable.get("DO_AWS_CREDNTIALS_access_token")
    secret_key = Variable.get("DO_AWS_CREDNTIALS_secret_token")

    s3 = boto3.client(
        "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )
    bucket = "main-billbox"
    response = s3.get_object(Bucket=bucket, Key=s3_file)
    df = pd.read_json(response.get("Body"), lines=True)

    df["_id"] = df["_id"].astype(str).str.lstrip("{'$oid': '")
    df["_id"] = df["_id"].astype(str).str.strip("'}")
    # {'$date': '2015-05-07T16:08:00.201Z'}
    # Replacing the empty date fields because transforming nan in SQL was a problem
    df["dateModified"] = df["dateModified"].fillna("")
    df["dateCreated"] = df["dateCreated"].fillna("")

    # Stripping the date objects to have only the date string
    df["dateCreated"] = df["dateCreated"].astype(str).str.lstrip("{'$date': '")
    df["dateCreated"] = df["dateCreated"].astype(str).str.strip("'}")

    df["dateModified"] = df["dateModified"].astype(str).str.lstrip("{'$date': '")
    df["dateModified"] = df["dateModified"].astype(str).str.strip("'}")

    df.to_csv(local_file, index=False)
    df_header = pd.read_csv(local_file, nrows=0).columns.tolist()
    df_header_edit = [str(item) + " character varying(500)" for item in df_header]

    Variable.set("do_download_file", local_file)

    creation_script = "".join(str(x) + "," for x in df_header_edit)
    Variable.set(
        "table_create_sql",
        "CREATE TABLE IF NOT EXISTS do_billbox.public.billbox_accounts ( {} )".format(
            creation_script[:-1]
        ),
    )
    return
