from typing_extensions import assert_type
import boto3
from moto import mock_s3
from unittest import mock
from airflow.models import Variable
import pandas as pd


@mock_s3
def test_file():
    with (mock.patch.dict(
        "os.environ",
        AIRFLOW_VAR_DO_AWS_CREDNTIALS_ACCESS_TOKEN="AARWK"),
        mock.patch.dict(
        "os.environ",
        AIRFLOW_VAR_DO_AWS_CREDNTIALS_SECRET_TOKEN="AD2PBioe"),
        mock.patch.dict(
            "os.environ",
            AIRFLOW_VAR_DO_BUCKET_NAME="main-billbox")):
        from ..dags.do_read import ReadFromS3
        s3 = boto3.client("s3")
        assert s3, "S3 connection not established"
        s3.create_bucket(Bucket="main-billbox")
        s3.put_object(
            Key="backups/billbox_account_20220713.json",
            Bucket='main-billbox')
        assert_type(ReadFromS3().read_file_object(
            "backups/billbox_account_20220713.json"), pd.DataFrame)
    # assert True


if __name__ == '__main__':
    test_file()
