"""
Project: desiging a data pipeline to 
A. Scrape data from the corresponding Website to extract hourly pedestrian counts 
     for Melbourne suburbs, obtained by sensors, in get_data(..) function.
   This data includes the following fields:

 data = {
        "id",
        "date_time",
        "year",
        "month",
        "mdate",
        "day",
        "time",
        "sensor_id",
        "sensor_name",
        "hourly_counts",
    }
   1) main data has 4320533 records: "https://data.melbourne.vic.gov.au/api/views/b2ak-trbp/rows.csv?accessType=DOWNLOAD&api_foundry=true",
   2) test data is a subset of the main data with only 1000 records (smaller csv file): "https://data.melbourne.vic.gov.au/resource/b2ak-trbp.csv",
   The execution time is significantly different for these two datasets, 
    Therefore, if you want to see a quick output please use the second link; otherwise use the first link and allow aws to some time to store data in S3 bucket.
 B. Transform data so that we get top 10 suburbs with highest pedestrian count per month and per day separately.
    Functions process_df_top_10_day() and process_df_top_10_month() perform those actions, respectively.

 C. Upload result to S3
"""
"""
Optional: _get_key function extracts date and time for use in the csv file name 
 so that each time the script is run, a new file is created on S3 rather overwriting the previous file.
However, I commented this out for now to save space on S3
"""

""" 
Get_data(..) extracts data to a temporary file which can be used later in this script
"""
"""
process_df_top_10_day() finds 10 suburbs with highest number of pedestrian per each day.
"""

import boto3
import pandas as pd
import csv

# from datetime import datetime, timezone
from pandas import read_csv
from io import StringIO


s3 = boto3.resource("s3")
bucket = "soheila-s3-bucket"
LOCAL_FILE_SYS = "/tmp"
FILE_NAME = LOCAL_FILE_SYS + "/" + "data"


Number_of_Records = 4320533
DF_TOP_10_day = {}
DF_TOP_10_month = {}
DF = {}


# def _get_key():
#     dt_now = datetime.now(tz=timezone.utc)
#     KEY = (
#         dt_now.strftime("%Y-%m-%d")
#         + "/"
#         + dt_now.strftime("%H")
#         + "/"
#         + dt_now.strftime("%M")
#         + "/"
#     )
#     return KEY


def get_data(
    get_path="https://data.melbourne.vic.gov.au/resource/b2ak-trbp.csv",
    # 1) main data with 4320533 records "https://data.melbourne.vic.gov.au/api/views/b2ak-trbp/rows.csv?accessType=DOWNLOAD&api_foundry=true",
    # 2) a subset of the main data with only 1000 records (smaller csv file): "https://data.melbourne.vic.gov.au/resource/b2ak-trbp.csv",
):

    try:

        DF = pd.read_csv(get_path)
        data_list = DF.values.tolist()

        header = [
            "id",
            "date_time",
            "year",
            "month",
            "mdate",
            "day",
            "time",
            "sensor_id",
            "sensor_name",
            "hourly_counts",
        ]
        with open(FILE_NAME, "w", encoding="UTF8") as f:

            writer = csv.writer(f)

            # write the header
            writer.writerow(header)
            for row in data_list:
                # write the data
                writer.writerow(row)

    except KeyError as e:
        print(f"Wrong format url {get_path}", e)


def process_df_top_10_day():
    try:

        DF_local_day = pd.read_csv(FILE_NAME)
        DF_sum_day = (
            DF_local_day.groupby(["year", "month", "mdate", "sensor_id", "sensor_name"])
            .agg({"hourly_counts": "sum"})
            .reset_index()
        )
        DF_sum_day = DF_sum_day.rename(columns={"hourly_counts": "pedestrain_counts"})

        DF_sorted_day = (
            DF_sum_day.groupby(["year", "month", "mdate"])
            .apply(lambda x: x.sort_values(["pedestrain_counts"], ascending=False))
            .reset_index(drop=True)
        )
        # choosing top 10 suburb and their records
        DF_TOP_10_day = DF_sorted_day.groupby(["year", "month", "mdate"]).head(10)
        # inserting row_id (incremental key)
        DF_TOP_10_day.insert(0, "row_id", range(0, len(DF_TOP_10_day)))
        # creating rank which shows the rank of each suburbs within the year/month/day group
        DF_TOP_10_day["rank"] = DF_TOP_10_day.groupby(["year", "month", "mdate"])[
            "pedestrain_counts"
        ].rank("dense", ascending=False)
        df = pd.DataFrame(DF_TOP_10_day)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, encoding="utf-8", index=False)
        s3_resource = boto3.resource("s3")
        s3_resource.Object(bucket, "top_10_day.csv").put(Body=csv_buffer.getvalue())

    except:
        print("output is OK..")


def process_df_top_10_month():
    try:
        DF_local_month = pd.read_csv(FILE_NAME)

        DF_sum_month = (
            DF_local_month.groupby(["year", "month", "sensor_id", "sensor_name"])
            .agg({"hourly_counts": "sum"})
            .reset_index()
        )
        DF_sum_month = DF_sum_month.rename(
            columns={"hourly_counts": "pedestrain_counts"}
        )

        DF_sorted_month = (
            DF_sum_month.groupby(["year", "month"])
            .apply(lambda x: x.sort_values(["pedestrain_counts"], ascending=False))
            .reset_index(drop=True)
        )
        # choosing top 10 suburb and their records
        DF_TOP_10_month = DF_sorted_month.groupby(["year", "month"]).head(10)
        # inserting row_id (incremental key)
        DF_TOP_10_month.insert(0, "row_id", range(0, len(DF_TOP_10_month)))
        # creating rank which shows the rank of each suburbs within the year/month group
        DF_TOP_10_month["rank"] = DF_TOP_10_month.groupby(["year", "month"])[
            "pedestrain_counts"
        ].rank("dense", ascending=False)
        df = pd.DataFrame(DF_TOP_10_month)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, encoding="utf-8", index=False)
        s3_resource = boto3.resource("s3")
        s3_resource.Object(bucket, "top_10_month.csv").put(Body=csv_buffer.getvalue())
    except:
        print("output is OK..")


def lambda_handler(event, context):
    get_data()
    process_df_top_10_day()
    process_df_top_10_month()
