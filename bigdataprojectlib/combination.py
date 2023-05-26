# combination.py
# The third step of the pipeline

# Importing necessary libraries
import json
import findspark
from datetime import datetime, timedelta

findspark.init()  # Initializing Spark

from pyspark.sql import SparkSession
from pyspark.sql import functions as Func
from pyspark.sql import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

from requests import Session

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
    .config("spark.sql.session.timeZone", "UTC")
    .appName("spark_localstack_news_data")
    .getOrCreate()
)


def predict_close_price(prices_key, prices_data_frame):
    """
    This function takes a key of a JSON file stored in S3 bucket and a Spark DataFrame containing the prices data,
    loads the file into a Spark DataFrame and creates a linear regression model to predict the next day close price.

    Args:
        prices_key (str): The key of the file stored in S3 containing the prices data.
        prices_data_frame (DataFrame): The Spark DataFrame containing the prices data.

    Returns:
        prices_data_frame (DataFrame): The Spark DataFrame containing the prices data with the predicted close price.
        Prints an error message if an exception is encountered.
    """
    print(f"Predicting close price for {prices_key}")
    try:
        yesterday = datetime.today() - timedelta(days=1)
        date = yesterday.strftime("%Y-%m-%d")

        # Create a features vector
        assembler = VectorAssembler(
            inputCols=[
                "open",
                "high",
                "low",
                "volume",
                "dividend_amount",
                "split_coefficient",
            ],
            outputCol="features",
        )
        prices_data_frame = assembler.transform(prices_data_frame)

        # Shift the close prices to create a "next day" close price column
        prices_data_frame = prices_data_frame.withColumn(
            "next_day_close",
            Func.lead("close").over(Window.partitionBy("symbol").orderBy("date")),
        )

        # Split the data into training and test sets
        training_data_frame = prices_data_frame.filter(prices_data_frame.date < date)
        test_data_frame = prices_data_frame.filter(prices_data_frame.date <= date)

        # Define the linear regression model
        lr = LinearRegression(featuresCol="features", labelCol="next_day_close")

        # Fit the model to the data
        lr_model = lr.fit(training_data_frame.na.drop())

        # Make predictions
        prediction = lr_model.transform(test_data_frame)

        prediction = prediction.withColumn(
            "date", Func.to_date(Func.col("date"), "yyyy-MM-dd")
        )

        # Add 1 day to the date
        prediction = prediction.withColumn("date", Func.date_add(Func.col("date"), 1))

        # Add the predicted close price to the DataFrame
        prices_data_frame = prices_data_frame.join(
            prediction.select("date", "prediction"), on="date", how="right"
        )

        return prices_data_frame
    except Exception as error:
        print(error)


def combine_news_and_prices(news_key, prices_key):
    """
    This function takes 2 keys of JSON files stored in S3 bucket and combines the data from the files into a single

    Args:
        news_key (str): The key of the file stored in S3 containing the news data.
        prices_key (str): The key of the file stored in S3 containing the prices data.

    Returns:
        parquet_key (str): The key of the file stored in S3 containing the combined data.
        Prints an error message if an exception is encountered.
    """
    print(f"Combining {news_key} and {prices_key} data...")
    try:
        # Read the parquet files into DataFrames
        news_data_frame = spark_session.read.parquet(
            f"s3a://big-data-project-formatting/{news_key}"
        )
        prices_data_frame = spark_session.read.parquet(
            f"s3a://big-data-project-formatting/{prices_key}"
        )
        prices_data_frame = prices_data_frame.withColumn(
            "date", Func.to_date(Func.col("date"), "yyyy-MM-dd")
        )

        # Predict the close price
        prices_data_frame = predict_close_price(prices_key, prices_data_frame)

        prices_data_frame.show()

        # Convert the time_published column to date in news_df
        news_data_frame = news_data_frame.withColumn(
            "date", Func.to_date(Func.col("time_published"), "yyyy-MM-dd")
        )

        news_data_frame.show()

        # Group the news data and calculate the required columns
        news_data_frame = (
            news_data_frame.groupBy("date")
            .agg(
                Func.sum(
                    Func.when(
                        Func.col("ticker_sentiment_label") == "Bullish", 1
                    ).otherwise(0)
                ).alias("positive_news_amount"),
                Func.sum(
                    Func.when(
                        Func.col("ticker_sentiment_label") == "Bearish", 1
                    ).otherwise(0)
                ).alias("negative_news_amount"),
                Func.mean("ticker_sentiment_score").alias(
                    "ticker_sentiment_score_mean"
                ),
                Func.mean("overall_sentiment_score").alias(
                    "overall_sentiment_score_mean"
                ),
            )
            .withColumn(
                "ticker_sentiment_score_mean_label",
                Func.when(Func.col("ticker_sentiment_score_mean") >= 0.35, "Bullish")
                .when(Func.col("ticker_sentiment_score_mean") <= -0.35, "Bearish")
                .otherwise("Neutral"),
            )
            .withColumn(
                "overall_sentiment_score_mean_label",
                Func.when(Func.col("overall_sentiment_score_mean") >= 0.35, "Bullish")
                .when(Func.col("overall_sentiment_score_mean") <= -0.35, "Bearish")
                .otherwise("Neutral"),
            )
        )

        # Combine the news and prices data
        combined_data_frame = (
            prices_data_frame.alias("p")
            .join(
                news_data_frame.alias("n"),
                (Func.col("p.date") == Func.col("n.date")),
            )
            .select(
                Func.col("p.symbol").alias("symbol"),
                Func.col("p.date"),
                Func.col("p.close").alias("close_price"),
                Func.col("p.prediction").alias("close_price_prediction"),
                "positive_news_amount",
                "negative_news_amount",
                "ticker_sentiment_score_mean",
                "ticker_sentiment_score_mean_label",
                "overall_sentiment_score_mean",
                "overall_sentiment_score_mean_label",
            )
        )

        # If the combined DataFrame is empty, add only the news data
        if combined_data_frame.count() == 0:
            combined_data_frame = news_data_frame.select(
                Func.col("date"),
                Func.lit(0).alias("close_price"),
                Func.lit(0).alias("close_price_prediction"),
                "positive_news_amount",
                "negative_news_amount",
                "ticker_sentiment_score_mean",
                "ticker_sentiment_score_mean_label",
                "overall_sentiment_score_mean",
                "overall_sentiment_score_mean_label",
            )
        
        # Get the symbol from the key
        symbol = news_key.split("_")[0]

        # Update the symbol column
        combined_data_frame = combined_data_frame.withColumn("symbol", Func.lit(symbol))

        # Convert the date to the current date with the correct format UTC
        combined_data_frame = combined_data_frame.withColumn(
            "timestamp", Func.unix_timestamp(Func.current_timestamp())*1000
        )

        # Fetch the current price
        session = Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        response = session.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?region=FR&lang=fr-FR&includePrePost=false&interval=5m&useYfid=true&range=1d&corsDomain=fr.finance.yahoo.com&.tsrc=finance"
        )
        current_price = json.loads(response.text)["chart"]["result"][0]["meta"][
            "regularMarketPrice"
        ]

        # Add the current price to the DataFrame
        combined_data_frame = combined_data_frame.withColumn(
            "current_price", Func.lit(current_price)
        )

        combined_data_frame.show()

        # Save the DataFrame as a Parquet file
        parquet_key = (
            f'{news_key.rsplit(".", 1)[0]}.parquet'  # Remove the .parquet extension
        )
        combined_data_frame.write.parquet(
            f"s3a://big-data-project-combination/{parquet_key}"
        )

        return parquet_key

    except Exception as error:
        print("Error: ", error)
        print("keys: ", news_key, prices_key)


def combine_all_data(news_keys, prices_keys):
    """
    This function takes 2 arrays of keys and combines the news and prices data for each pair of keys.

    Args:
        news_keys (list): The keys of the files stored in S3 containing the news data (one key per symbol
        prices_keys (list): The keys of the files stored in S3 containing the prices data (one key per symbol)

    Returns:
        parquet_keys (list): The keys of the files stored in S3 containing the combined data (one key per symbol)
        Prints an error message if an exception is encountered.
    """
    parquet_keys = []
    print("Combining news and prices data...")
    for news_key, prices_key in zip(news_keys, prices_keys):
        parquet_key = combine_news_and_prices(news_key, prices_key)
        parquet_keys.append(parquet_key)
    print("Done combining news and prices data.")
    return parquet_keys
