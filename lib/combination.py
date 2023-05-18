import json
import findspark
from datetime import datetime, timedelta

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

from requests import Session

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


def predict_close_price(key, df):
    print(f"Predicting close price for {key}")
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
        df = assembler.transform(df)

        # Shift the close prices to create a "next day" close price column
        df = df.withColumn(
            "next_day_close",
            F.lead("close").over(Window.partitionBy("symbol").orderBy("date")),
        )

        # Split the data into training and test sets
        train_df = df.filter(df.date < date)
        test_df = df.filter(df.date <= date)

        # Define the linear regression model
        lr = LinearRegression(featuresCol="features", labelCol="next_day_close")

        # Fit the model to the data
        lr_model = lr.fit(train_df.na.drop())

        # Make predictions
        prediction = lr_model.transform(test_df)

        prediction = prediction.withColumn(
            "date", F.to_date(F.col("date"), "yyyy-MM-dd")
        )

        # Décalage temporel de 1 jour
        prediction = prediction.withColumn(
            "date", F.date_add(F.col("date"), 1)
        )

        # Add the predicted close price to the DataFrame
        df = df.join(prediction.select("date", "prediction"), on="date", how="right")

        return df
    except Exception as e:
        print(e)


def combine_news_prices(news_key, prices_key):
    print(f"Combining {news_key} and {prices_key} data...")
    try:
        # Read the parquet files into DataFrames
        news_df = spark.read.parquet(f"s3a://big-data-project-formatting/{news_key}")
        prices_df = spark.read.parquet(
            f"s3a://big-data-project-formatting/{prices_key}"
        )

        # Predict the close price
        prices_df = predict_close_price(prices_key, prices_df)

        # Convert the time_published column to date in news_df
        news_df = news_df.withColumn("date", F.to_date(F.col("time_published")))

        # Group the news data and calculate the required columns
        news_df = (
            news_df.groupBy("date")
            .agg(
                F.sum(
                    F.when(F.col("ticker_sentiment_label") == "Bullish", 1).otherwise(0)
                ).alias("positive_news_amount"),
                F.sum(
                    F.when(F.col("ticker_sentiment_label") == "Bearish", 1).otherwise(0)
                ).alias("negative_news_amount"),
                F.mean("ticker_sentiment_score").alias("ticker_sentiment_score_mean"),
                F.mean("overall_sentiment_score").alias("overall_sentiment_score_mean"),
            )
            .withColumn(
                "ticker_sentiment_score_mean_label",
                F.when(F.col("ticker_sentiment_score_mean") > 0, "Bullish").otherwise(
                    "Bearish"
                ),
            )
            .withColumn(
                "overall_sentiment_score_mean_label",
                F.when(F.col("overall_sentiment_score_mean") > 0, "Bullish").otherwise(
                    "Bearish"
                ),
            )
        )

        # Combine the news and prices data
        combined_df = (
            prices_df.alias("p")
            .join(
                news_df.alias("n"),
                (F.col("p.date") == F.col("n.date")),
            )
            .select(
                F.col("p.symbol").alias("symbol"),
                F.col("p.date"),
                F.col("p.close").alias("close_price"),
                F.col("p.prediction").alias("close_price_prediction"),
                "positive_news_amount",
                "negative_news_amount",
                "ticker_sentiment_score_mean",
                "ticker_sentiment_score_mean_label",
                "overall_sentiment_score_mean",
                "overall_sentiment_score_mean_label",
            )
        )

        # Get the symbol from the key
        symbol = news_key.split("_")[0]

        # Update the symbol column
        combined_df = combined_df.withColumn("symbol", F.lit(symbol))

        # Convert the date to the current date with the correct format(yyyy-MM-dd hh:mm:ss)    
        combined_df = combined_df.withColumn(
            "date", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )

        # Fetch the current price
        session = Session()
        session.headers.update({'User-Agent': 'Mozilla/5.0'})
        response = session.get(f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?region=FR&lang=fr-FR&includePrePost=false&interval=5m&useYfid=true&range=1d&corsDomain=fr.finance.yahoo.com&.tsrc=finance')
        current_price = json.loads(response.text)["chart"]["result"][0]["meta"]["regularMarketPrice"]

        # Add the current price to the DataFrame
        combined_df = combined_df.withColumn("current_price", F.lit(current_price)) 

        combined_df.show()

        # Save the DataFrame as a Parquet file
        parquet_key = (
            f'{news_key.rsplit(".", 1)[0]}.parquet'  # Remove the .parquet extension
        )
        combined_df.write.parquet(f"s3a://big-data-project-combination/{parquet_key}")

        return parquet_key

    except Exception as e:
        print("Error: ", e)
        print("keys: ", news_key, prices_key)


def combine_all(news_keys, prices_keys):
    parquet_keys = []
    print("Combining news and prices data...")
    for news_key, prices_key in zip(news_keys, prices_keys):
        parquet_key = combine_news_prices(news_key, prices_key)
        parquet_keys.append(parquet_key)
    print("Done combining news and prices data.")
    return parquet_keys
