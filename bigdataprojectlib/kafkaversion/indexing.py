# indexing.py
# The last step of the pipeline

# Importing necessary libraries
import json
import findspark

findspark.init()  # Initializing Spark

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
)
from elasticsearch import Elasticsearch

from confluent_kafka import Consumer, Producer, KafkaError


# Creates a SparkSession instance, the entry point to all functionality in Spark
spark_session = (
    SparkSession.builder.master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .appName("spark_indexing")
    .getOrCreate()
)

# Create an Elasticsearch instance
elasticsearch_client = Elasticsearch(
    [{"host": "localhost", "port": 9200, "scheme": "http"}]
)

# Consumer configuration
consumer_conf = {
    "bootstrap.servers": "localhost:29092",
    "group.id": "indexing_group",
    "auto.offset.reset": "earliest",
}

# Creating a Kafka Consumer
consumer = Consumer(consumer_conf)


def index_data_into_elasticsearch(data_json):
    """
    Indexes the data into Elasticsearch

    Args:
        data_json (str): The json data of a specific ticker

    """
    print(f"Indexing data...")
    # Loads data into a DataFrame
    raw_data_frame = spark_session.read.json(
        spark_session.sparkContext.parallelize(data_json)
    )

    data_frame = raw_data_frame.select(
        raw_data_frame["symbol"],
        raw_data_frame["date"].cast(TimestampType()).alias("date"),
        raw_data_frame["close_price"].cast(DoubleType()),
        raw_data_frame["close_price_prediction"].cast(DoubleType()),
        raw_data_frame["positive_news_amount"].cast(IntegerType()),
        raw_data_frame["negative_news_amount"].cast(IntegerType()),
        raw_data_frame["ticker_sentiment_score_mean"].cast(DoubleType()),
        raw_data_frame["ticker_sentiment_score_mean_label"].cast(StringType()),
        raw_data_frame["overall_sentiment_score_mean"].cast(DoubleType()),
        raw_data_frame["overall_sentiment_score_mean_label"].cast(StringType()),
        raw_data_frame["timestamp"].cast(TimestampType()),
        raw_data_frame["current_price"].cast(DoubleType()),
    )

    # Converts the DataFrame to a list of dictionaries
    data_list = [row.asDict() for row in data_frame.collect()]

    # Gets the ticker from the key in lowercase
    stock_ticker = data_frame.select("symbol").first()[0].lower()
    print(f"Ticker: {stock_ticker}")

    # Creates the index if it doesn't exist
    if not elasticsearch_client.indices.exists(index=stock_ticker):
        elasticsearch_client.indices.create(index=stock_ticker)

    # Indexes the data into Elasticsearch
    for doc in data_list:
        elasticsearch_client.index(index=stock_ticker, document=doc)

    print(f"Data indexed successfully!")


consumer.subscribe(["combine_data_topic"])

# Consume messages
while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    # Load data from message
    data = json.loads(msg.value().decode("utf-8"))

    # Index data into Elasticsearch
    index_data_into_elasticsearch(data)

# Close down consumer and producer to commit final offsets.
consumer.close()
