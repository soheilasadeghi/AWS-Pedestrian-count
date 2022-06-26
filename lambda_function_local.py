"""
This script generates the output LOCALLY for the main dataset including 4320533
This cn be performed by the instruction given in the readme file to load data to S3.
However, since it is possible that this take a bit longer than 15 min to run, might not be always suitable 
to be rperformed by lambda function. I am going to write a unix cron job to execute this code with a second method.

"""

import boto3

from datetime import datetime, timezone
from pandas import read_csv
import pandas as pd

import csv
from io import StringIO

s3 = boto3.resource("s3")
bucket = "soheila-s3-bucket"
LOCAL_FILE_SYS = "/tmp"
FILE_NAME = LOCAL_FILE_SYS + "/" + "data"
# tempfile = "/Users/soheilasadeghi/Desktop/lambda_s3/temp.csv"
tempfile = "temp.csv"


Number_of_Records = 4320533
DF_TOP_10_day = {}
DF_TOP_10_month = {}
DF = {}


def get_data(
    get_path="https://data.melbourne.vic.gov.au/api/views/b2ak-trbp/rows.csv?accessType=DOWNLOAD&api_foundry=true",
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
        with open(tempfile, "w", encoding="UTF8") as f:

            writer = csv.writer(f)

            # write the header
            writer.writerow(header)
            for row in data_list:
                # write the data
                writer.writerow(row)

    except KeyError as e:
        print(f"Wrong format url {get_path}", e)


def process_df_top_10_day():

    # try:

    DF_local_day = pd.read_csv(tempfile)
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

    DF_TOP_10_day = DF_sorted_day.groupby(["year", "month", "mdate"]).head(10)
    DF_TOP_10_day.insert(0, "row_id", range(0, len(DF_TOP_10_day)))

    DF_TOP_10_day["rank"] = DF_TOP_10_day.groupby(["year", "month", "mdate"])[
        "pedestrain_counts"
    ].rank("dense", ascending=False)
    print(DF_TOP_10_day)

    df = pd.DataFrame(DF_TOP_10_day)
    # csv_buffer = StringIO()
    df.to_csv(
        "out_day.csv",
        encoding="utf-8",
        index=False,
    )
    # s3_resource = boto3.resource("s3")
    # s3_resource.Object(bucket, "top_10_day.csv").put(Body=csv_buffer.getvalue())

    # except:
    print("output is OK..")


def process_df_top_10_month():
    # try:
    DF_local_month = pd.read_csv(tempfile)

    DF_sum_month = (
        DF_local_month.groupby(["year", "month", "sensor_id", "sensor_name"])
        .agg({"hourly_counts": "sum"})
        .reset_index()
    )
    DF_sum_month = DF_sum_month.rename(columns={"hourly_counts": "pedestrain_counts"})

    DF_sorted_month = (
        DF_sum_month.groupby(["year", "month"])
        .apply(lambda x: x.sort_values(["pedestrain_counts"], ascending=False))
        .reset_index(drop=True)
    )
    DF_TOP_10_month = DF_sorted_month.groupby(["year", "month"]).head(10)

    DF_TOP_10_month.insert(0, "row_id", range(0, len(DF_TOP_10_month)))

    DF_TOP_10_month["rank"] = DF_TOP_10_month.groupby(["year", "month"])[
        "pedestrain_counts"
    ].rank("dense", ascending=False)

    df = pd.DataFrame(DF_TOP_10_month)
    print(df)
    df.to_csv(
        "\out_month.csv",
        encoding="utf-8",
        index=False,
    )
    # except:
    print("output is OK..")


def main():
    get_data()
    process_df_top_10_day()
    process_df_top_10_month()


if __name__ == "__main__":
    main()
