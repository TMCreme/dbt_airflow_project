import requests
from requests import JSONDecodeError
from bs4 import BeautifulSoup
import configparser
import datetime
import csv
import boto3
from botocore.exceptions import NoCredentialsError
from airflow.models import Variable

"""
This web scraping logic howbeit general is specifically
tailored to how the https://www.jumia.com.gh
website handles their routing.
The category data is stored in a dictionary with the
format {"name":"link/href"}
The category data dictionary will be used to
get the various products. Since the routing
doesn't consider the sub-categories, I will
be not use the sub categories because
I will get the products when I use the category pages.
Using the category links, the product data
will be taken and parsed into a CSV to be stored on AWS S3
"""


# Reading credentials from config.ini
def read_creds(subject, key):
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config[subject][key]


# getting the raw data from the website
def getdata(url):
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    return r.text


# using beautiful soup to scrap the html data
def html_code(url):
    htmldata = getdata(url)
    # print(htmldata)
    soup = BeautifulSoup(htmldata, "html.parser")
    return soup


# getting the categories listed on the website in a dictionary as name:link
# I will use the links for the various products on each category page
def category_data(soup):
    print("Initiating Category Scrapping")
    data_str = {}
    bUrl = "https://www.jumia.com.gh"
    for item in soup.find_all("a", class_="itm"):
        if item.has_attr("href"):
            if item["href"][:4] != "http":
                data_str[item.get_text()] = bUrl + item["href"]
            else:
                data_str[item.get_text()] = item["href"]
    print("Category scrapping done")
    return data_str


# This function gets the individual product information into a dictionary
# the data here is in raw form and needs cleaning
def product_by_category(cat):
    print("Starting Product Scrapping")
    data_dict = {}
    for k, v in cat.items():
        soup = html_code(v)
        data_dict[k] = []
        for item in soup.find_all("article", class_="prd _box col _hvr"):
            data_dict[k].append(item.get_text())
    print("Products scrapping done. Proceeding to Clean the data")
    clean_product_data(data_dict)
    return data_dict


#  This functions does the initial cleaning
#  The product scrapping takes the entire product information
def clean_product_data(product):
    product_json = {}
    for k, v in product.items():
        product_json[k] = []
        for item in v:
            price_ind = item.index("GH₵")
            type_data = item[:price_ind].split("-")[-1]
            name_data = item[: item.index(type_data)]
            currency = item[price_ind : price_ind + 3]
            price_data = item[price_ind + 4 : price_ind + 9]
            percent_data = item[-3:]
            product_json[k].append(
                {
                    "name": name_data,
                    "type": type_data,
                    "price": price_data,
                    "percent": percent_data,
                    "currency": currency,
                }
            )
    print(product_json)
    write_to_csv(product_json)


# This function converts the jsondata into a csv file
def write_to_csv(jsondata):
    filename = datetime.datetime.now().strftime("%Y%m%d%H")
    keys = ["date", "category", "name", "price", "percent", "currency"]
    try:
        with open("transforms/" + filename + ".csv", "w", newline="") as output_file:
            dict_writer = csv.writer(output_file)
            dict_writer.writerow(keys)
            for k, v in jsondata.items():
                # new_df = {}
                for items in v:
                    items["price"] = float(items["price"].replace(",", ""))
                    items["percent"] = float(items["percent"].strip("%"))
                    item_date = datetime.datetime.strptime(filename, "%Y%m%d%H")
                    dict_writer.writerow(
                        [
                            item_date,
                            k,
                            (items["name"] + items["type"]).replace(",", "-"),
                            items["price"],
                            items["percent"],
                            items["currency"],
                        ]
                    )
        upload_to_aws("transforms/" + filename + ".csv", filename + ".csv")
    except JSONDecodeError as jse:
        print(jse)


# This function uploads the file to the S3 bucket on AWS
def upload_to_aws(local_file, s3_file):
    access_key = read_creds("AWS_CREDENTIALS", "aws_access_key")
    secret_key = read_creds("AWS_CREDENTIALS", "aws_secret_key")
    s3 = boto3.resource(
        "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )
    bucket = "transforme-products"
    try:
        s3.meta.client.upload_file(local_file, bucket, s3_file)
        Variable.set("latest_uploaded_file", local_file[11:])
    except FileNotFoundError:
        print("The file was not found")
        return None
    except NoCredentialsError:
        print("Credentials not available")
        return None
    except Exception as otherExcept:
        print(otherExcept)


if __name__ == "__main__":
    url = "https://www.jumia.com.gh/"
    print("Starting the execution")
    soup = html_code(url)
    # print(soup)
    catetories = category_data(soup)
    sub_categories = product_by_category(catetories)
