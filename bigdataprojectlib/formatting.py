# formatting.py
# The second step of the pipeline

# Importing necessary libraries
import datetime
import findspark

findspark.init()  # Initializing Spark

from pyspark.sql import SparkSession
from pyspark.sql import functions as Func

# Creating a SparkSession
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
    .appName("spark_localstack_formatting")
    .getOrCreate()
)


def format_news_data(key):
    """
    This function takes a key of a JSON file stored in S3 bucket, loads the file into a Spark DataFrame,
    transforms the DataFrame and finally writes it back to S3 as a Parquet file.

    Args:
        key (str): The key of the file stored in S3 containing the news data.

    Returns:
        str: The key of the file stored in S3 containing the transformed news data in Parquet format.
        Prints an error message if an exception is encountered.
    """
    print(f"Formatting {key}")
    try:
        # Read JSON data into DataFrame
        data_frame = spark_session.read.json(f"s3a://big-data-project-ingestion/{key}")

        # Extract ticker from key
        ticker = key.split("_")[0]

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

        data_frame.show()

        # Convert "time_published" to UTC timestamp format
        data_frame = data_frame.withColumn(
            "time_published", Func.from_utc_timestamp(Func.to_timestamp(Func.col("time_published"), "yyyyMMdd'T'HHmmss"), "UTC")
        )

        # Show DataFrame
        data_frame.show()

        # Write DataFrame to Parquet format and save in S3
        parquet_file_key = f'{key.rsplit(".", 1)[0]}.parquet'
        data_frame.write.parquet(
            f"s3a://big-data-project-formatting/{parquet_file_key}"
        )

        return parquet_file_key

    except Exception as error:
        print("Error: ", error)
        print("key: ", key)


def format_prices_data(key):
    """
    This function takes a key of a JSON file stored in S3 bucket, loads the file into a Spark DataFrame,
    transforms the DataFrame and finally writes it back to S3 as a Parquet file.

    Args:
        key (str): The key of the file stored in S3 containing the stock prices data.

    Returns:
        str: The key of the file stored in S3 containing the transformed stock prices data in Parquet format.
        Prints an error message if an exception is encountered.
    """
    print(f"Formatting {key}")
    try:
        # Read JSON data into DataFrame
        data_frame = spark_session.read.json(f"s3a://big-data-project-ingestion/{key}")

        # Extract symbol from DataFrame
        symbol = data_frame.select(data_frame["Meta Data"]["2. Symbol"]).first()[0]

        # Transform DataFrame
        rdd = data_frame.select("`Time Series (Daily)`").rdd.flatMap(lambda x: x)

        rdd = rdd.flatMap(lambda x: [(symbol, k, v) for k, v in x.asDict().items()])

        data_frame = rdd.toDF(["symbol", "date", "values"])

        data_frame = data_frame.select(
            Func.col("symbol"),
            Func.from_utc_timestamp(Func.col("date"), "UTC").alias("date"),
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

        # Write DataFrame to Parquet format and save in S3
        parquet_file_key = f'{key.rsplit(".", 1)[0]}.parquet'
        data_frame.write.parquet(
            f"s3a://big-data-project-formatting/{parquet_file_key}"
        )

        return parquet_file_key

    except Exception as error:
        print("Error: ", error)
        print("key: ", key)


def format_all_news(keys):
    """
    This function formats all news data using the function 'format_news_data'.

    Args:
        keys (list): List of keys of the files stored in S3 containing the news data.

    Returns:
        list: List of keys of the files stored in S3 containing the transformed news data in Parquet format.
    """
    parquet_file_keys = []
    print("Formatting news data...")
    for key in keys:
        parquet_file_key = format_news_data(key)
        parquet_file_keys.append(parquet_file_key)
    print("Done formatting news data.")
    return parquet_file_keys


def format_all_prices(keys):
    """
    This function formats all stock prices data using the function 'format_prices_data'.

    Args:
        keys (list): List of keys of the files stored in S3 containing the stock prices data.

    Returns:
        list: List of keys of the files stored in S3 containing the transformed stock prices data in Parquet format.
    """
    parquet_file_keys = []
    print("Formatting prices data...")
    for key in keys:
        parquet_file_key = format_prices_data(key)
        parquet_file_keys.append(parquet_file_key)
    print("Done formatting prices data.")
    return parquet_file_keys
