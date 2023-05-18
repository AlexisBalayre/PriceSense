import os

import findspark
from datetime import datetime, timedelta

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from elasticsearch import Elasticsearch

spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config(
        "spark.jars.packages",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hadoop:hadoop-aws:3.3.4",
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", True)
    .appName("spark_localstack_news_data")
    .getOrCreate()
)

es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

def index_data_by_key(key):
    print(f"Indexing {key}")
    # Load the data into a DataFrame
    df = spark.read.parquet(f"s3a://big-data-project-combination/{key}")

    # Convert the DataFrame to a list of dictionaries
    data = [row.asDict() for row in df.collect()]

    #get the ticker from the key in lowercase
    ticker = key.split("_")[0].lower()

    # Create the index if it doesn't exist
    if not es.indices.exists(index=ticker):
        es.indices.create(index=ticker)

    # Index the data into Elasticsearch
    for doc in data:
        es.index(index=ticker, body=doc)

def index_data(keys):
    print("Indexing data...")
    for key in keys:
        index_data_by_key(key)
    print("Done indexing data.")



