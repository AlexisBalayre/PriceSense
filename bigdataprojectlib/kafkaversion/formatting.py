# formatting.py
# The second step of the pipeline

# Importing necessary libraries
import json
import findspark

findspark.init()  # Initializing Spark

from pyspark.sql import SparkSession
from pyspark.sql import functions as Func
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType
from confluent_kafka import Consumer, Producer, KafkaError

# Creating a SparkSession
spark_session = (
    SparkSession.builder.master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .appName("spark_formatting")
    .getOrCreate()
)

# Consumer configuration
consumer_conf = {
    "bootstrap.servers": "localhost:29092",
    "group.id": "formatting_group",
    "auto.offset.reset": "earliest",
}

# Producer configuration
producer_conf = {
    "bootstrap.servers": "localhost:29092",
}

# Creating a Kafka Consumer
consumer = Consumer(consumer_conf)

# Creating a Kafka Producer
producer = Producer(producer_conf)


def format_news_data(json_data):
    """
    This function formats news of a specific ticker

    Args:
        ticker (str): The stock symbol for which to format news data.
    """
    print(f"Formatting news data...")
    try:
        # Define the schema for the topics array
        topics_schema = ArrayType(
            StructType(
                [
                    StructField("relevance_score", StringType(), True),
                    StructField("topic", StringType(), True),
                ]
            )
        )

        # Define the schema for the ticker_sentiment array
        ticker_sentiment_schema = ArrayType(
            StructType(
                [
                    StructField("ticker", StringType(), True),
                    StructField("relevance_score", StringType(), True),
                    StructField("ticker_sentiment_score", StringType(), True),
                    StructField("ticker_sentiment_label", StringType(), True),
                ]
            )
        )

        # Define the schema for the JSON
        json_schema = StructType(
            [
                StructField("items", StringType(), True),
                StructField("sentiment_score_definition", StringType(), True),
                StructField("relevance_score_definition", StringType(), True),
                StructField(
                    "feed",
                    ArrayType(
                        StructType(
                            [
                                StructField("title", StringType(), True),
                                StructField("url", StringType(), True),
                                StructField("time_published", StringType(), True),
                                StructField(
                                    "authors", ArrayType(StringType(), True), True
                                ),
                                StructField("summary", StringType(), True),
                                StructField("banner_image", StringType(), True),
                                StructField("source", StringType(), True),
                                StructField(
                                    "category_within_source", StringType(), True
                                ),
                                StructField("source_domain", StringType(), True),
                                StructField("topics", topics_schema, True),
                                StructField(
                                    "overall_sentiment_score", DoubleType(), True
                                ),
                                StructField(
                                    "overall_sentiment_label", StringType(), True
                                ),
                                StructField(
                                    "ticker_sentiment", ticker_sentiment_schema, True
                                ),
                            ]
                        )
                    ),
                    True,
                ),
                StructField("ticker", StringType(), True),
            ]
        )

        # Parse the JSON using the defined schema
        data_frame = spark_session.read.json(
            spark_session.sparkContext.parallelize([json.dumps(json_data)]),
            schema=json_schema,
        )

        # Extract ticker from DataFrame
        ticker = data_frame.select(data_frame["ticker"]).first()[0]

        # Transform DataFrame
        data_frame = (
            data_frame.selectExpr("explode(feed) as feed")
            .selectExpr(
                "feed.time_published",
                "explode(feed.ticker_sentiment) as ticker_sentiment",
                "feed.overall_sentiment_score",
                "feed.overall_sentiment_label",
            )
            .selectExpr(
                "time_published",
                "ticker_sentiment.ticker",
                "ticker_sentiment.ticker_sentiment_score",
                "ticker_sentiment.ticker_sentiment_label",
                "ticker_sentiment.relevance_score",
                "overall_sentiment_score",
                "overall_sentiment_label",
            )
            .filter(f"ticker = '{ticker}'")
        )

        # Convert "time_published" to UTC timestamp format
        data_frame = data_frame.withColumn(
            "time_published",
            Func.to_utc_timestamp(
                Func.to_timestamp(Func.col("time_published"), "yyyyMMdd'T'HHmmss"),
                "UTC",
            ),
        )

        # Show DataFrame
        data_frame.show()

        # Convert DataFrame to JSON and send it to Kafka
        producer.produce(
            "format_news_topic",
            json.dumps(data_frame.toJSON().collect()).encode("utf-8"),
        )

    except Exception as error:
        print("Error: ", error)


def format_prices_data(json_data):
    """
    This function formats prices of a specific ticker

    Args:
        json_data (str): the json data of a specific ticker
    """
    print(f"Formatting stock prices data...")
    try:
        # Transform JSON data into DataFrame
        data_frame = spark_session.createDataFrame([json_data])

        # Extract symbol from DataFrame
        symbol = data_frame.select(data_frame["Meta Data"]["2. Symbol"]).first()[0]

        # Extract the time series data from the DataFrame

        rdd = data_frame.select("`Time Series (Daily)`").rdd.flatMap(lambda x: x)
        # Transform RDD to DataFrame
        rdd = rdd.flatMap(lambda x: [(symbol, k, v) for k, v in x.items()])
        data_frame = rdd.toDF(["symbol", "date", "values"])

        data_frame = data_frame.select(
            Func.col("symbol"),
            Func.to_utc_timestamp(Func.col("date"), "UTC").alias("date"),
            data_frame["values"]["1. open"].cast("double").alias("open"),
            data_frame["values"]["2. high"].cast("double").alias("high"),
            data_frame["values"]["3. low"].cast("double").alias("low"),
            data_frame["values"]["4. close"].cast("double").alias("close"),
            data_frame["values"]["5. adjusted close"]
            .cast("double")
            .alias("adjusted_close"),
            data_frame["values"]["6. volume"].cast("double").alias("volume"),
            data_frame["values"]["7. dividend amount"]
            .cast("double")
            .alias("dividend_amount"),
            data_frame["values"]["8. split coefficient"]
            .cast("double")
            .alias("split_coefficient"),
        )

        # Show DataFrame
        data_frame.show()

        # Convert DataFrame to JSON and send it to Kafka
        producer.produce(
            "format_prices_topic",
            json.dumps(data_frame.toJSON().collect()).encode("utf-8"),
        )

    except Exception as error:
        print("Error: ", error)


# Subscribing the consumer to both the 'ingest_prices_topic' and 'ingest_news_topic' topics.
consumer.subscribe(["ingest_prices_topic", "ingest_news_topic"])

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

    try:
        data = json.loads(msg.value().decode("utf-8"))

    except Exception as e:
        print("Error while parsing:", e)
        break

    if msg.topic() == "ingest_prices_topic":
        format_prices_data(data)
    elif msg.topic() == "ingest_news_topic":
        format_news_data(data)


# Close down consumer and producer to commit final offsets.
consumer.close()
producer.flush()
