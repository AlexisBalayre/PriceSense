# ingestion.py
# The first step of the pipeline

# Importing required libraries
import os
import json
from datetime import datetime
import time

# Importing networking and messaging libraries
from requests import Session
from confluent_kafka import Producer

# Importing environment variable handling library
from dotenv import load_dotenv

load_dotenv()  # Loading the environment variables

# Producer configuration
producer_conf = {
    "bootstrap.servers": "localhost:29092",
}

# Creating a Kafka Producer
producer = Producer(producer_conf)


def fetch_alpha_vantage_news(ticker, time_from, limit):
    """
    Fetch news data from Alpha Vantage for a specific ticker.

    Args:
        ticker (str): The stock symbol for which to fetch news.
        time_from (str): The starting date from which to fetch news.
        limit (int): The maximum number of news items to fetch.
    """
    # The URL to fetch data from Alpha Vantage API
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&time_from={time_from}&limit={limit}&apikey={os.environ.get("ALPHAVANTAGE_API_KEY")}'

    session = Session()  # Creating a new session to manage connections

    try:
        # Attempt to send a GET request to the Alpha Vantage API
        response = session.get(url)

        # Return the JSON response if the request was successful
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Request failed with exception {e}")
    finally:
        session.close()  # Always ensure the session is closed, even in case of error


def fetch_alpha_vantage_stock_data(ticker):
    """
    Fetch stock market data from Alpha Vantage for a specific ticker.

    Args:
        ticker (str): The stock symbol for which to fetch stock market data.

    """
    # The URL to fetch data from Alpha Vantage API
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={ticker}&apikey={os.environ.get("ALPHAVANTAGE_API_KEY")}'

    session = Session()  # Creating a new session to manage connections

    try:
        # Attempt to send a GET request to the Alpha Vantage API
        response = session.get(url)

        # Return the JSON response if the request was successful
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Request failed with exception {e}")
    finally:
        session.close()  # Always ensure the session is closed, even in case of error


def ingest_news_to_kafka(ticker, limit):
    """
    Fetch news data for a specific ticker and send it to Kafka.

    Args:
        ticker (str): The stock symbol for which to fetch and store news.
        limit (int): The maximum number of news items to fetch.
    """
    print(f"Ingesting news for {ticker}...")
    now = datetime.now()  # Current date and time
    time_from = now.strftime("%Y%m%dT0000")  # Format current date and time
    # Fetch news data
    news_data = fetch_alpha_vantage_news(ticker, time_from, limit)
    if news_data is not None:
        # Add the ticker to the news data
        news_data["ticker"] = ticker
        # Produce a new message to the Kafka topic 'ingest_news_topic'
        producer.produce("ingest_news_topic", json.dumps(news_data))
        print(f"News for {ticker} ingested successfully!")


def ingest_stock_prices_to_kafka(ticker):
    """
    Fetch stock market data for a specific ticker and send it to Kafka.

    Args:
        ticker (str): The stock symbol for which to fetch and store stock market data.
    """
    print(f"Ingesting stock prices for {ticker}...")
    # Fetch stock data
    stock_data = fetch_alpha_vantage_stock_data(ticker)
    if stock_data is not None:
        # Produce a new message to the Kafka topic 'ingest_prices_topic'
        producer.produce("ingest_prices_topic", json.dumps(stock_data))
        print(f"Stock prices for {ticker} ingested successfully!")


"""
Here is the good way to do. However due to the limited 
number of Alphavantage API calls with a free API key, 
we will hardcode here the values of the 2 api calls 
using the values present in the json files of the 'test' folder. 

tickers = ["AAPL", "GOOG"]
limit = 200
for ticker in tickers:
    ingest_news_to_kafka(ticker, 200)
    ingest_stock_prices_to_kafka(ticker)
    time.sleep(4)  # Delay before ingesting next data
"""


# Loading data from JSON files and ingesting to Kafka
aapl_prices_json = {}
goog_prices_json = {}
aapl_news_json = {}
goog_news_json = {}

# Reading data from 'AAPL' prices JSON file
with open("test/aapl_prices.json") as f:
    read_data = f.read()
    aapl_prices_json = json.loads(read_data)
    f.close()

# Reading data from 'GOOG' prices JSON file
with open("test/goog_prices.json") as f:
    read_data = f.read()
    goog_prices_json = json.loads(read_data)
    f.close()

# Reading data from 'AAPL' news JSON file
with open("test/aapl_news.json") as f:
    read_data = f.read()
    aapl_news_json = json.loads(read_data)
    f.close()

# Reading data from 'GOOG' news JSON file
with open("test/goog_news.json") as f:
    read_data = f.read()
    goog_news_json = json.loads(read_data)
    f.close()

# Continuously ingest data to Kafka
while True:
    print(f"Ingesting data for AAPL...")
    # Ingest 'AAPL' data to Kafka
    producer.produce("ingest_news_topic", json.dumps(aapl_news_json))
    producer.produce("ingest_prices_topic", json.dumps(aapl_prices_json))

    print(f"Ingesting data for GOOG...")
    time.sleep(10)  # Delay before ingesting 'GOOG' data
    # Ingest 'GOOG' data to Kafka
    producer.produce("ingest_news_topic", json.dumps(goog_news_json))
    producer.produce("ingest_prices_topic", json.dumps(goog_prices_json))

    print(f"Data for ingested successfully!")
