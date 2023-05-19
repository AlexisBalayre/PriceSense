from bigdataprojectlib.ingestion import ingest_all_news, ingest_all_stock_prices
from bigdataprojectlib.formatting import format_all_news, format_all_prices
from bigdataprojectlib.combination import combine_all_data
from bigdataprojectlib.indexing import index_all_s3_data

# Ingest news data
news_keys = ingest_all_news()
prices_keys = ingest_all_stock_prices()

# Format news data
news_parquet_keys = format_all_news(news_keys)
prices_parquet_keys = format_all_prices(prices_keys)

# Combine news and stock prices data
keys = combine_all_data(news_parquet_keys, prices_parquet_keys)

# Index all data
index_all_s3_data(keys)