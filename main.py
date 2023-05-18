from lib.ingestion import ingest_news, ingest_prices
from lib.formatting import format_news, format_prices
from lib.combination import combine_all

news_keys = ingest_news()
news_parquet_keys = format_news(news_keys)

prices_keys = ingest_prices()
prices_parquet_keys = format_prices(prices_keys)

keys = combine_all(news_parquet_keys, prices_parquet_keys)
print(keys)


