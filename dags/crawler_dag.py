import requests
from requests import JSONDecodeError
from bs4 import BeautifulSoup 
from datetime import timedelta, datetime 
from airflow import DAG 
import csv
from airflow.models import Variable
from airflow.utils.dates import days_ago 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator



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
def category_data():
    bUrl = "https://www.jumia.com.gh"
    soup = html_code(bUrl)
    print("Initiating Category Scrapping")
    data_str = {}
    for item in soup.find_all("a", class_="itm"):
        if item.has_attr("href"):
            if item["href"][:4] != "http":
                data_str[item.get_text()] = bUrl + item["href"]
            else:
                data_str[item.get_text()] = item["href"]
    print("Category scrapping done")
    product_by_category(data_str)
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
    filename = datetime.now().strftime("%Y%m%d%H")
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
                    item_date = datetime.strptime(filename, "%Y%m%d%H")
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
        Variable.set("local_file_full_path", "transforms/"+filename+".csv")
        Variable.set("latest_uploaded_file", filename+".csv")
    except JSONDecodeError as jse:
        print(jse)



# Define the default arguments for the DAG 
# default_args = {
#     "owner" : "airflow",
#     "depends_on_past" : False,
#     "start_date" : days_ago(2),
#     "email" : ["admin"],
#     "email_on_failure" : False,
#     "email_on_retry" : False,
#     "retries" : 0,
#     "retry_delay" : timedelta(minutes=5)
# }


# This is a simple dag definition for the purpose of this mini project 
# dag = DAG(
#     "crawler_dag",
#     default_args=default_args,
#     description="Crawling products from a website",
#     schedule_interval=timedelta(days=2)
# )

# crawling_data = PythonOperator(
#     task_id="crawl_data_from_online_shop",
#     python_callable=category_data,
#     dag=dag,
# )


# copy_to_s3 = LocalFilesystemToS3Operator(
#     task_id="copy_file_to_s3",
#     filename=Variable.get("local_file_full_path"),
#     aws_conn_id="s3_conn",
#     dest_bucket=Variable.get("destination_bucket_name"),
#     dest_key=Variable.get("latest_uploaded_file"),
#     dag=dag
# )


# crawling_data >> copy_to_s3



