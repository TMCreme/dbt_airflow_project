import requests
from bs4 import BeautifulSoup
# from lxml import etree
import json, configparser 
import datetime, time, os
import boto3
from botocore.exceptions import NoCredentialsError

"""
This web scraping logic howbeit general is specifically tailored to how the https://www.jumia.com.gh
website handles their routing. 
The category data is stored in a dictionary with the format {"name":"link/href"}
The category data dictionary will be used to get the various products. Since the routing 
doesn't consider the sub-categories, I will be not use the sub categories because 
I will get the products when I use the category pages.
Using the category links, the product data will be taken and parsed into a CSV to be stored on AWS S3 
"""


# Reading credentials from config.ini
def read_creds(subject, key):
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config[subject][key]

# getting the raw data from the website 
def getdata(url):
    r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"})
    return r.text


# using beautiful soup to scrap the html data 
def html_code(url):
    htmldata = getdata(url)
    # print(htmldata)
    soup = BeautifulSoup(htmldata,"html.parser")
    return soup

# getting the categories listed on the website in a dictionary as name:link
# I will use the links for the various products on each category page
def category_data(soup):
    print("Initiating Category Scrapping")
    data_str = {}
    for item in soup.find_all("a", class_="itm"):
        if item.has_attr('href'):
            if item["href"][:4] != "http":
                data_str[item.get_text()] = "https://www.jumia.com.gh" + item["href"]
            else:
                data_str[item.get_text()] = item["href"]
        # else:
        #     data_str[item.get_text()] = ""
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


"""
 This functions does the initial cleaning 
 The product scrapping takes the entire product information 
 """
def clean_product_data(product):
    product_json = {}
    for k, v in product.items():
        product_json[k] = []
        for item in v:
            price_ind = item.index("GH₵")
            type_data = item[:price_ind].split("-")[-1]
            name_data = item[:item.index(type_data)]
            currency = item[price_ind:price_ind+3]
            price_data = item[price_ind+4:price_ind+9]
            percent_data = item[-3:]
            product_json[k].append({"name":name_data,"type": type_data,"price":price_data,"percent":percent_data,"currency":currency})
    print(product_json)
    write_to_json(product_json)
    


# the function to convert the json data to a json file
def write_to_json(jsondata):
    filename = datetime.datetime.now().strftime("%Y%m%d%H")
    with open("raw_data/"+filename+".json", "w", encoding='utf-8') as f:
        json.dump(jsondata, f, ensure_ascii=False, indent=4)
    upload_to_aws("raw_data/"+filename+".json",filename+".json")
    return True
        
def upload_to_aws(local_file, s3_file):
    access_key = read_creds("AWS_CREDENTIALS","aws_access_key")
    secret_key = read_creds("AWS_CREDENTIALS","aws_secret_key")
    s3 = boto3.resource('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_key)
    bucket = "jumia-product-info"
    try:
        s3.meta.client.upload_file(local_file, bucket, s3_file)     
    except FileNotFoundError:
        print("The file was not found")
        return None
    except NoCredentialsError:
        print("Credentials not available")
        return None


if __name__ == "__main__":
    url = "https://www.jumia.com.gh/"
    print("Starting the execution")
    soup = html_code(url)
    # print(soup)

    catetories = category_data(soup)
    sub_categories = product_by_category(catetories)