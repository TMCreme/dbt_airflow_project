from dags.do_read import ReadFromS3
import pytest
import pandas as pd
import os
# This is fix import errors from the do_read.py
import sys
sys.path.append("..")


# Creating a test file fixture for the tests
@pytest.fixture
def clean_file_mock(tmp_path):
    rootdir = os.path.dirname(os.path.abspath(__file__))
    # Included a sample data to mimick the real data
    test_file = os.path.join(rootdir, 'test_data.json')
    with open(test_file) as outfile:
        target_output = outfile.read()
        test_df = pd.read_json(target_output, lines=True)
        print(test_df.keys())
    return target_output


# Testing the existence of the fields that needed transformation
# the first is dateModified
def test_date_modified(clean_file_mock):
    cleaning = ReadFromS3(
    ).clean_up_file(
        clean_file_mock
    )
    assert "dateModified" in cleaning.keys()


# Testing the existence of the fields that needed transformation
# the second is datecreated
def test_date_created(clean_file_mock):
    cleaning = ReadFromS3(
    ).clean_up_file(
        clean_file_mock
    )
    assert "dateCreated" in cleaning.keys()
    assert type(cleaning) == pd.DataFrame


# Testing the existence of the fields that needed transformation
# the third is _id
def test_id(clean_file_mock):
    cleaning = ReadFromS3(
    ).clean_up_file(
        clean_file_mock
    )
    assert "_id" in cleaning.keys()


# Testing the type of data returned from cleaning
def test_function_type(clean_file_mock):
    cleaning = ReadFromS3(
    ).clean_up_file(
        clean_file_mock
    )
    assert type(cleaning) == pd.DataFrame


# Testing the type of data returned.
def test_save_file(clean_file_mock):
    saving = ReadFromS3().save_to_csv(
        clean_file_mock,
        "/tmp/test_file.csv"
    )

    assert type(saving) == dict


# Testing the exact size of the dict returned
def test_dict_size(clean_file_mock):
    saving = ReadFromS3().save_to_csv(
        clean_file_mock,
        "/tmp/test_file.csv"
    )
    assert len(saving) == 2
