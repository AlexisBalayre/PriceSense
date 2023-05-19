# ingestion.py
# The first step of the pipeline

# Importing required libraries
import os
import json
from datetime import datetime

from requests import Session
from confluent_kafka import Producer

from dotenv import load_dotenv

load_dotenv()  # Loading the environment variables

p = Producer({'bootstrap.servers': 'mybroker'})

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


def ingest_news_to_kafka(ticker, limit):
    """
    Fetch news data for a specific ticker and send it to Kafka.

    Args:
        ticker (str): The stock symbol for which to fetch and store news.
        limit (int): The maximum number of news items to fetch.
    """
    now = datetime.now()
    time_from = now.strftime("%Y%m%dT0000")
    news_data = fetch_alpha_vantage_news(ticker, time_from, limit)
    if news_data is not None:
        p.produce(f'ingest_{ticker}_news_topic', json.dumps(news_data).encode('utf-8'))


def ingest_stock_prices_to_kafka(ticker):
    """
    Fetch stock market data for a specific ticker and send it to Kafka.

    Args:
        ticker (str): The stock symbol for which to fetch and store stock market data.
    """
    stock_data = fetch_alpha_vantage_stock_data(ticker)
    if stock_data is not None:
        p.produce(f'ingest_{ticker}_prices_topic', json.dumps(stock_data).encode('utf-8'))


def ingest_all_news():
    """
    Fetch and send news data for a list of tickers to Kafka.
    """
    tickers = ["AAPL", "GOOG"]
    limit = 200
    for ticker in tickers:
        ingest_news_to_kafka(ticker, limit)


def ingest_all_stock_prices():
    """
    Fetch and send stock market data for a list of tickers to Kafka.
    """
    tickers = ["AAPL", "GOOG"]
    for ticker in tickers:
        ingest_stock_prices_to_kafka(ticker)
