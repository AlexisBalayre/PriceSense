from requests import Request, Session

import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

import json
from datetime import datetime, timedelta
import boto3

def get_news(
    ticker,
    time_from,
    limit
):
    api_token = 'TCRMZMBRSCOP9GFH'
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&time_from={time_from}&limit={limit}&apikey={api_token}'
    session = Session()    
    response = session.get(url)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f'Request failed with status code {response.status_code}')
        return None
    

s3 = boto3.client(
    's3', 
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test'
)

def fetcher(ticker, limit):
    now = datetime.now()
    ten_minutes_ago = now - timedelta(hours=5)
    time_from = ten_minutes_ago.strftime("%Y%m%dT%H%M")
    news_data = get_news(ticker, time_from, limit)
    key = f'{ticker}_{now.strftime("%Y-%m-%dT%H-%M-%S")}.json'
    s3.put_object(Bucket='mybucket', Key=key, Body=json.dumps(news_data))
    return key  # return the key for later use

key = fetcher('AAPL', 50)
print("key: ", key)

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("spark_localstack_news_data") \
    .getOrCreate()

def formatter(spark, bucket, key):
    # Read the JSON file directly into a DataFrame
    df = spark.read.json(f"s3a://{bucket}/{key}")

    # Flatten the DataFrame and filter for the desired ticker
    df = df.selectExpr("explode(feed) as feed")\
        .selectExpr("feed.time_published", 
                    "explode(feed.ticker_sentiment) as ticker_sentiment", 
                    "feed.overall_sentiment_score", 
                    "feed.overall_sentiment_label")\
        .selectExpr("time_published",
                    "ticker_sentiment.ticker",
                    "ticker_sentiment.ticker_sentiment_score",
                    "ticker_sentiment.ticker_sentiment_label",
                    "ticker_sentiment.relevance_score",
                    "overall_sentiment_score",
                    "overall_sentiment_label")\
        .filter("ticker = 'AAPL'")

    # Convert the time_published field to a proper timestamp
    df = df.withColumn("time_published", F.to_timestamp(F.col("time_published"), "yyyyMMdd'T'HHmmss"))

    # Show the DataFrame
    df.show()

    # Save the DataFrame as a Parquet file
    df.write.parquet(f's3a://{bucket}/formatted_{key}.parquet')

    
formatter(spark, 'mybucket', key)  # use the returned key from the fetcher function 