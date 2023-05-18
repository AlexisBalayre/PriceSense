import os
import json
from datetime import datetime, timedelta

from requests import Session
import boto3

from dotenv import load_dotenv

load_dotenv()


s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
)


def get_news_data(ticker, time_from, limit):
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&time_from={time_from}&limit={limit}&apikey={os.environ.get("ALPHAVANTAGE_API_KEY")}'
    print(url)
    try:
        session = Session()
        response = session.get(url)
        if response.status_code == 200:
            data = response.json()
            print(data)
            return data
        else:
            print(f"Request failed with status code {response.status_code}")
            return None
    except Exception as e:
        print(f"Request failed with exception {e}")
        return None
    finally:
        session.close()


def get_stocks_data(ticker):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={ticker}&apikey={os.environ.get("ALPHAVANTAGE_API_KEY")}'
    print(url)
    try:
        session = Session()
        response = session.get(url)
        if response.status_code == 200:
            data = response.json()
            print(data)
            return data
        else:
            print(f"Request failed with status code {response.status_code}")
            return None
    except Exception as e:
        print(f"Request failed with exception {e}")
        return None
    finally:
        session.close()


def fetch_news(ticker, limit):
    print(f"Fetching news for {ticker}")
    try:
        now = datetime.now()
        time_from = now.strftime("%Y%m%dT0000")
        news_data = get_news_data(ticker, time_from, limit)
        key = f'{ticker}_news_{now.strftime("%Y-%m-%dT%H-%M-%S")}.json'
        s3.put_object(
            Bucket="big-data-project-ingestion", Key=key, Body=json.dumps(news_data)
        )
        return key  # return the key for later use
    except Exception as e:
        print(f"Request failed with exception {e}")
        return None


def fetch_prices(ticker):
    print(f"Fetching prices for {ticker}")
    try:
        stocks_data = get_stocks_data(ticker)
        key = f'{ticker}_prices_{datetime.now().strftime("%Y-%m-%dT%H-%M-%S")}.json'
        s3.put_object(
            Bucket="big-data-project-ingestion", Key=key, Body=json.dumps(stocks_data)
        )
        return key  # return the key for later use
    except Exception as e:
        print(f"Request failed with exception {e}")
        return None


def ingest_news():
    tickers = ["AAPL", "GOOG"]
    limit = 200
    keys = []
    print("Ingesting news")
    for ticker in tickers:
        key = fetch_news(ticker, limit)
        keys.append(key)
    print("Done ingesting news")
    return keys


def ingest_prices():
    tickers = ["AAPL", "GOOG"]
    keys = []
    print("Ingesting prices")
    for ticker in tickers:
        key = fetch_prices(ticker)
        keys.append(key)
    print("Done ingesting prices")
    return keys
