# ingestion.py
# The first step of the pipeline

# Importing required libraries
import os
import json
from datetime import datetime
from requests import Session
import boto3
from dotenv import load_dotenv

load_dotenv()  # Loading the environment variables

# Creating S3 client to store news data
s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
)


def fetch_alpha_vantage_news(ticker, time_from, limit):
    """
    Fetch news data from Alpha Vantage for a specific ticker.

    Args:
        ticker (str): The stock symbol for which to fetch news.
        time_from (str): The starting date from which to fetch news.
        limit (int): The maximum number of news items to fetch.

    Returns:
        dict: The news data for the specified ticker in dictionary format.
        Returns None if an error occurred.
    """
    # The URL to fetch data from Alpha Vantage API
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&time_from={time_from}&limit={limit}&apikey={os.environ.get("ALPHAVANTAGE_API_KEY")}'
    session = Session()
    try:
        response = session.get(url)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Request failed with exception {e}")
    finally:
        session.close()
    return None


def fetch_alpha_vantage_stock_data(ticker):
    """
    Fetch stock market data from Alpha Vantage for a specific ticker.

    Args:
        ticker (str): The stock symbol for which to fetch stock market data.

    Returns:
        dict: The stock market data for the specified ticker in dictionary format.
        Returns None if an error occurred.
    """
    # The URL to fetch data from Alpha Vantage API
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={ticker}&apikey={os.environ.get("ALPHAVANTAGE_API_KEY")}'
    session = Session()
    try:
        response = session.get(url)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Request failed with exception {e}")
    finally:
        session.close()
    return None


def ingest_news_to_s3(ticker, limit):
    """
    Fetch and store news data for a specific ticker in S3.

    Args:
        ticker (str): The stock symbol for which to fetch and store news.
        limit (int): The maximum number of news items to fetch.

    Returns:
        str: The key of the file stored in S3 containing the news data.
        Returns None if an error occurred.
    """
    now = datetime.now()
    time_from = now.strftime("%Y%m%dT0000")
    news_data = fetch_alpha_vantage_news(ticker, time_from, limit)
    if news_data is not None:
        key = f'{ticker}_news_{now.strftime("%Y-%m-%dT%H-%M-%S")}.json'
        s3_client.put_object(
            Bucket="big-data-project-ingestion", Key=key, Body=json.dumps(news_data)
        )
        return key
    return None


def ingest_stock_prices_to_s3(ticker):
    """
    Fetch and store stock market data for a specific ticker in S3.

    Args:
        ticker (str): The stock symbol for which to fetch and store stock market data.

    Returns:
        str: The key of the file stored in S3 containing the stock market data.
        Returns None if an error occurred.
    """
    stock_data = fetch_alpha_vantage_stock_data(ticker)
    if stock_data is not None:
        key = f'{ticker}_prices_{datetime.now().strftime("%Y-%m-%dT%H-%M-%S")}.json'
        s3_client.put_object(
            Bucket="big-data-project-ingestion", Key=key, Body=json.dumps(stock_data)
        )
        return key
    return None


def ingest_all_news():
    """
    Fetch and store news data for a list of tickers in S3.

    Returns:
        list: List of keys of the files stored in S3 containing the news data.
    """
    tickers = ["AAPL", "GOOG"]
    limit = 200
    keys = [ingest_news_to_s3(ticker, limit) for ticker in tickers]
    return keys


def ingest_all_stock_prices():
    """
    Fetch and store stock market data for a list of tickers in S3.

    Returns:
        list: List of keys of the files stored in S3 containing the stock market data.
    """
    tickers = ["AAPL", "GOOG"]
    keys = [ingest_stock_prices_to_s3(ticker) for ticker in tickers]
    return keys
