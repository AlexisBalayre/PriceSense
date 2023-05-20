# combination.py
# The third step of the pipeline

# Importing necessary libraries
import json
from datetime import datetime, timedelta
import findspark

findspark.init()  # Initializing Spark

from pyspark.sql import SparkSession
from pyspark.sql import functions as Func
from pyspark.sql import Window
from pyspark.sql.types import (
    DoubleType,
    TimestampType,
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

from confluent_kafka import Consumer, Producer, KafkaError

from requests import Session


# Creating a SparkSession
# This represents the connection to a Spark cluster and can be used to create DataFrame, register DataFrame as tables,
# execute SQL over tables, cache tables, and read parquet files.
spark_session = (
    SparkSession.builder.master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .appName("spark_combination")
    .getOrCreate()
)

# Kafka consumer configuration
# Bootstrap servers define the initial connections for the consumer to locate the Kafka cluster.
# Group id is a unique string that identifies the consumer group this consumer belongs to.
# auto.offset.reset determines what to do when there is no initial offset in Kafka.
consumer_conf = {
    "bootstrap.servers": "localhost:29092",
    "group.id": "combination_group",
    "auto.offset.reset": "earliest",
}

# Creating a Kafka Consumer to consume messages from Kafka broker.
consumer = Consumer(consumer_conf)

# Kafka producer configuration
# Bootstrap servers define the initial connections for the producer to locate the Kafka cluster.
producer_conf = {
    "bootstrap.servers": "localhost:29092",
}

# Creating a Kafka Producer to produce messages to Kafka broker.
producer = Producer(producer_conf)


def predict_close_price(ticker, prices_data_frame):
    """
    This function takes a Spark DataFrame of stock price data and predicts the close price for a specific ticker.
    The prediction is made using a linear regression model trained on the input data.

    Args:
        ticker (str): The ticker for which to predict the close price.
        prices_data_frame (DataFrame): The Spark DataFrame containing the prices data.

    Returns:
        prices_data_frame (DataFrame): The Spark DataFrame containing the prices data with the predicted close price.
        Prints an error message if an exception is encountered.
    """
    print(f"Predicting close price for {ticker}")
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


def combine_news_and_prices(news_json, prices_json):
    """
    This function combines the news and prices data and sends it to the Kafka topic.
    It first reads the news and prices data into Spark DataFrames, transforms them to the correct format, and joins them on the date.
    Then it sends the combined data to the Kafka topic.

    Args:
        news_json (str): the json file that contains the news data
        prices_json (str): the json file that contains the prices data
    """
    print(f"Combining news and prices data...")
    try:
        prices_raw_data_frame = spark_session.read.json(
            spark_session.sparkContext.parallelize(prices_json)
        )
        news_raw_data_frame = spark_session.read.json(
            spark_session.sparkContext.parallelize(news_json)
        )

        prices_data_frame = prices_raw_data_frame.select(
            prices_raw_data_frame["symbol"],
            prices_raw_data_frame["date"].cast(TimestampType()).alias("date"),
            prices_raw_data_frame["open"].cast(DoubleType()),
            prices_raw_data_frame["high"].cast(DoubleType()),
            prices_raw_data_frame["low"].cast(DoubleType()),
            prices_raw_data_frame["close"].cast(DoubleType()),
            prices_raw_data_frame["adjusted_close"].cast(DoubleType()),
            prices_raw_data_frame["volume"].cast(DoubleType()),
            prices_raw_data_frame["dividend_amount"].cast(DoubleType()),
            prices_raw_data_frame["split_coefficient"].cast(DoubleType()),
        )

        news_data_frame = news_raw_data_frame.select(
            news_raw_data_frame["time_published"]
            .cast(TimestampType())
            .alias("time_published"),
            news_raw_data_frame["ticker"],
            news_raw_data_frame["ticker_sentiment_score"].cast(DoubleType()),
            news_raw_data_frame["ticker_sentiment_label"],
            news_raw_data_frame["relevance_score"].cast(DoubleType()),
            news_raw_data_frame["overall_sentiment_score"].cast(DoubleType()),
            news_raw_data_frame["overall_sentiment_label"],
        )

        prices_data_frame = prices_data_frame.withColumn(
            "date", Func.to_date(Func.col("date"), "yyyy-MM-dd")
        )

        ticker = news_data_frame.select("ticker").first()[0]

        # Predict the close price
        prices_data_frame = predict_close_price(ticker, prices_data_frame)

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

        # Update the symbol column
        combined_data_frame = combined_data_frame.withColumn("symbol", Func.lit(ticker))

        # Convert the date to the current date with the correct format UTC
        combined_data_frame = combined_data_frame.withColumn(
            "timestamp", Func.to_utc_timestamp(Func.current_timestamp(), "UTC")
        )

        # Fetch the current price
        session = Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        response = session.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?region=FR&lang=fr-FR&includePrePost=false&interval=5m&useYfid=true&range=1d&corsDomain=fr.finance.yahoo.com&.tsrc=finance"
        )
        current_price = json.loads(response.text)["chart"]["result"][0]["meta"][
            "regularMarketPrice"
        ]

        # Add the current price to the DataFrame
        combined_data_frame = combined_data_frame.withColumn(
            "current_price", Func.lit(current_price)
        )

        combined_data_frame.show()

        producer.produce(
            "combine_data_topic",
            json.dumps(combined_data_frame.toJSON().collect()),
        )

    except Exception as error:
        print("Error: ", error)


# Here we subscribe our consumer to the 'format_prices_topic' and 'format_news_topic' topics.
consumer.subscribe(["format_prices_topic", "format_news_topic"])

# These are placeholders for the prices and news data.
prices_json = None
news_json = None

# This is the main loop. It continually polls for new messages in the Kafka topics.
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

    # Get the topic of the consumed message
    topic = msg.topic()

    # Depending on the topic, process the message
    if topic == "format_prices_topic":
        print("Price data just arrived!")
        prices_json = json.loads(msg.value().decode("utf-8"))
    elif topic == "format_news_topic":
        print("News data just arrived!")
        news_json = json.loads(msg.value().decode("utf-8"))

    # Now, when you have both prices_key and news_key, you can combine them
    if prices_json and news_json:
        combine_news_and_prices(news_json, prices_json)
        prices_json = None
        news_json = None

# Close the Kafka consumer and flush any remaining messages in the producer.
consumer.close()
producer.flush()
