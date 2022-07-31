import pandas as pd 
import os, sys, json, csv, ast, datetime
from requests import JSONDecodeError
from crawler import read_creds
import boto3
from botocore.exceptions import NoCredentialsError


# transform json data into a compatible Athena format 
# 
def transform_json():
    listfiles =  ["raw_data/"+i for i in os.listdir("raw_data/")if i.startswith("2022")]
    print(listfiles)
    for eachfile in listfiles:
        try:
            f = open(eachfile)
            df = json.load(f)
            keys = ["date","category","name","price","percent","currency"]
            with open("transforms/"+eachfile[:-5]+".csv", 'w', newline='') as output_file:
                dict_writer = csv.writer(output_file)
                dict_writer.writerow(keys)
                # break
                for k, v in df.items():
                    # new_df = {}
                    for items in v:
                        items["price"] = float(items["price"].replace(",",""))
                        items["percent"] = float(items["percent"].strip("%"))
                        item_date = datetime.datetime.strptime(eachfile[9:-5],"%Y%m%d%H")
                        dict_writer.writerow([item_date,k,(items["name"]+items["type"]).replace(",","-"),
                            items["price"],items["percent"],items["currency"]])

            upload_to_aws("transforms/"+eachfile[:-5]+".csv", eachfile[9:-5]+".csv")
        except JSONDecodeError as jse: 
            pass
        # print(df)
        # with open("transforms/"+eachfile, "w", encoding='utf-8') as new_file:
        #     json.dump(dict_list, new_file, ensure_ascii=False, indent=4)
        
    # break


def upload_to_aws(local_file, s3_file):
    access_key = read_creds("AWS_CREDENTIALS","aws_access_key")
    secret_key = read_creds("AWS_CREDENTIALS","aws_secret_key")
    s3 = boto3.resource('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_key)
    bucket = "transforme-products"
    try:
        s3.meta.client.upload_file(local_file, bucket, s3_file)     
    except FileNotFoundError:
        print("The file was not found")
        return None
    except NoCredentialsError:
        print("Credentials not available")
        return None
# print(listfiles)


def json_to_csv():
    listfiles =  ["transforms/"+i for i in os.listdir('transforms/') if i.endswith(".json")]
    print(listfiles)
    for eachfile in listfiles:
        f = open(eachfile)
        df = json.loads(f)
        print(df)
        keys = df[0].keys()
        with open(eachfile[:-5]+".csv", 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(df)
        # print(df.head())
        break
        # df.to_csv()


# with open('people.csv', 'w', newline='') as output_file:
#     dict_writer = csv.DictWriter(output_file, keys)
#     dict_writer.writeheader()
#     dict_writer.writerows(to_csv)


if __name__ == "__main__":
    transform_json()
    # json_to_csv()