from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from bigdataprojectlib.ingestion import ingest_all_news, ingest_all_stock_prices
from bigdataprojectlib.formatting import format_all_news, format_all_prices
from bigdataprojectlib.combination import combine_all_data
from bigdataprojectlib.indexing import index_all_s3_data

default_args = {
    'owner': 'Alexis BALAYRE & Augustin CREUSILLET',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['alexis@balayre.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'big_data_pipeline',
    default_args=default_args,
    description='A simple pipeline for big data processing.',
    schedule_interval='@hourly',  # This can be changed to your needs
) as dag:

    def news_ingestion_callable():
        news_keys = ingest_all_news()
        return news_keys

    def news_formatting_callable(ti):
        news_keys = ti.xcom_pull(task_ids='ingestion_news')
        news_parquet_keys = format_all_news(news_keys)
        return news_parquet_keys

    def prices_ingestion_callable():
        prices_keys = ingest_all_stock_prices()
        return prices_keys

    def prices_formatting_callable(ti):
        prices_keys = ti.xcom_pull(task_ids='ingestion_prices')
        prices_parquet_keys = format_all_prices(prices_keys)
        return prices_parquet_keys

    def combination_callable(ti):
        news_parquet_keys = ti.xcom_pull(task_ids='formatting_news')
        prices_parquet_keys = ti.xcom_pull(task_ids='formatting_prices')
        keys = combine_all_data(news_parquet_keys, prices_parquet_keys)
        return keys

    def indexing_callable(ti):
        keys = ti.xcom_pull(task_ids='combination')
        index_all_s3_data(keys)

    ingestion_news_task = PythonOperator(
        task_id='ingestion_news',
        python_callable=news_ingestion_callable,
    )
    formatting_news_task = PythonOperator(
        task_id='formatting_news',
        python_callable=news_formatting_callable,
    )
    ingestion_prices_task = PythonOperator(
        task_id='ingestion_prices',
        python_callable=prices_ingestion_callable,
    )
    formatting_prices_task = PythonOperator(
        task_id='formatting_prices',
        python_callable=prices_formatting_callable,
    )
    combination_task = PythonOperator(
        task_id='combination',
        python_callable=combination_callable,
    )
    indexing_task = PythonOperator(
        task_id='indexing',
        python_callable=indexing_callable,
    )

    # Define the order in which the tasks should run
    ingestion_news_task >> formatting_news_task
    ingestion_prices_task >> formatting_prices_task
    [formatting_news_task, formatting_prices_task] >> combination_task >> indexing_task
