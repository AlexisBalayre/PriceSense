# indexing.py
# The last step of the pipeline

# Importing necessary libraries
import os
import findspark
from datetime import datetime, timedelta

findspark.init()  # Initializing Spark

from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch

# Creates a SparkSession instance, the entry point to all functionality in Spark
spark_session = (
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
    .appName("spark_localstack_indexing")
    .getOrCreate()
)

# Create an Elasticsearch instance
elasticsearch_client = Elasticsearch(
    [{"host": "localhost", "port": 9200, "scheme": "http"}]
)


def index_s3_data_into_elasticsearch(key):
    """
    Indexes the data into Elasticsearch for a specific key.

    Args:
        key (str): The key of the file stored in S3 containing the data to index.

    """
    print(f"Indexing {key}")
    # Loads data into a DataFrame
    data_frame = spark_session.read.parquet(f"s3a://big-data-project-combination/{key}")

    # Converts the DataFrame to a list of dictionaries
    data_list = [row.asDict() for row in data_frame.collect()]

    # Gets the ticker from the key in lowercase
    stock_ticker = key.split("_")[0].lower()

    # Creates the index if it doesn't exist
    if not elasticsearch_client.indices.exists(index=stock_ticker):
        elasticsearch_client.indices.create(index=stock_ticker)

    # Indexes the data into Elasticsearch
    for doc in data_list:
        elasticsearch_client.index(index=stock_ticker, body=doc)


def index_all_s3_data(keys):
    """
    Indexes the data into Elasticsearch for a list of keys.

    Args:
        keys (list): A list of keys of the files stored in S3 containing the data to index.

    """
    print("Indexing data...")
    for key in keys:
        index_s3_data_into_elasticsearch(key)
    print("Done indexing data.")
