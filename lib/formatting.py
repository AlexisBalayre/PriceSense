import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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


def news_formatter(key):
    print(f"Formatting {key}")
    try:
        # Read the JSON file directly into a DataFrame
        df = spark.read.json(f"s3a://big-data-project-ingestion/{key}")

        # Extract the ticker from the file name
        ticker = key.split("_")[0]  # Assuming the ticker is the first part of the file name

        # Flatten the DataFrame and filter for the desired ticker
        df = (
            df.selectExpr("explode(feed) as feed")
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

        # Convert the time_published field to a proper timestamp
        df = df.withColumn(
            "time_published",
            F.to_timestamp(F.col("time_published"), "yyyyMMdd'T'HHmmss"),
        )

        # Show the DataFrame
        df.show()

        # Save the DataFrame as a Parquet file
        parquet_key = f'{key.rsplit(".", 1)[0]}.parquet'  # Remove the .json extension
        df.write.parquet(f"s3a://big-data-project-formatting/{parquet_key}")

        # Return the parquet key
        return parquet_key

    except Exception as e:
        print("Error: ", e)
        print("key: ", key)


def prices_formatter(key):
    print(f"Formatting {key}")
    try:
        # Read the JSON file directly into a DataFrame
        df = spark.read.json(f"s3a://big-data-project-ingestion/{key}")

        # Get the symbol
        symbol = df.select(df["Meta Data"]["2. Symbol"]).first()[0]

        # Get the struct as an RDD
        rdd = df.select("`Time Series (Daily)`").rdd.flatMap(lambda x: x)

        # Process the RDD to convert the struct to a format we can work with
        rdd = rdd.flatMap(lambda x: [(symbol, k, v) for k, v in x.asDict().items()])

        # Convert the RDD back to a DataFrame
        df = rdd.toDF(["symbol", "date", "values"])

        # Expand the "values" column into separate columns
        df = df.select(
            F.col("symbol"),
            F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
            df["values"]["1. open"].cast("double").alias("open"),
            df["values"]["2. high"].cast("double").alias("high"),
            df["values"]["3. low"].cast("double").alias("low"),
            df["values"]["4. close"].cast("double").alias("close"),
            df["values"]["5. adjusted close"].cast("double").alias("adjusted_close"),
            df["values"]["6. volume"].cast("double").alias("volume"),
            df["values"]["7. dividend amount"].cast("double").alias("dividend_amount"),
            df["values"]["8. split coefficient"]
            .cast("double")
            .alias("split_coefficient"),
        )

        # Show the DataFrame
        df.show()

        # Save the DataFrame as a Parquet file
        parquet_key = f'{key.rsplit(".", 1)[0]}.parquet'  # Remove the .json extension
        df.write.parquet(f"s3a://big-data-project-formatting/{parquet_key}")

        # Return the parquet key
        return parquet_key

    except Exception as e:
        print("Error: ", e)
        print("key: ", key)


def format_news(keys):
    parquet_keys = []
    print("Formatting news data...")
    for key in keys:
        parquet_key = news_formatter(key)
        parquet_keys.append(parquet_key)
    print("Done formatting news data.")
    return parquet_keys


def format_prices(keys):
    parquet_keys = []
    print("Formatting prices data...")
    for key in keys:
        parquet_key = prices_formatter(key)
        parquet_keys.append(parquet_key)
    print("Done formatting prices data.")
    return parquet_keys
