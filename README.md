# PriceSense: Correlating News Sentiment with Stock Prices

## Description

This project explores the correlation between financial news sentiment and stock prices. It employs two versions of a data pipeline: one leveraging LocalStack to emulate Amazon S3 for temporary data storage and message passing, and another using Apache Kafka for real-time data streaming and message passing. Both pipelines process raw financial news and stock prices, compute sentiment scores, combine the processed data, and index it into Elasticsearch for further analysis and visualization.

## Pipeline Overview

### Version 1: S3 Pipeline (emulated with LocalStack)

1. **Ingestion Phase**: The `bigdataprojectlib/ingestion.py` scripts fetch financial news and stock prices data respectively. The fetched data is stored in JSON format in emulated S3 buckets (provided by LocalStack) for the next phase.

2. **Formatting Phase**: The `bigdataprojectlib/formatting.py` scripts extract the raw data from the S3 buckets, format it into a suitable parquet format, and store it back into another set of S3 buckets for the combination phase.

3. **Combination Phase**: The `bigdataprojectlib/combination.py` script fetches the formatted news sentiment data and stock prices data from S3, combines them for each ticker symbol, and stores the combined data into an S3 bucket for the indexing phase.

4. **Indexing Phase**: The `bigdataprojectlib/indexing.py` script fetches the combined data from the S3 bucket, converts the parquet data into a Spark DataFrame, and indexes this data into Elasticsearch.

### Version 2: Kafka Pipeline

1. **Ingestion Phase**: The `bigdataprojectlib/kafkaversion/ingestion.py` scripts are used to fetch financial news and stock prices data respectively. The fetched data is published to Kafka topics to signal data availability.

2. **Formatting Phase**: The `bigdataprojectlib/kafkaversion/formatting.py` scripts consume the Kafka topics from the ingestion phase. They format the raw data into JSON format and publish it to new Kafka topics for the combination phase.

3. **Combination Phase**: The `bigdataprojectlib/kafkaversion/combination.py` script consumes the Kafka topics from the formatting phase, combines the news sentiment data and the stock prices data for each ticker, and then publishes the combined data to a new Kafka topic for the indexing phase.

4. **Indexing Phase**: The `bigdataprojectlib/kafkaversion/indexing.py` script consumes the Kafka topic from the combination phase, converts the JSON data into a Spark DataFrame, and indexes this data into Elasticsearch.

## Getting Started

Follow these steps to get the project running on your local machine:

1. Clone the repository to your local machine.
2. Create a virtual environment and activate it: `python3 -m venv venv && source venv/bin/activate`
3. Install the required packages using the following command: `pip install -r requirements.txt`
4. Build the Python package using the following command: `python setup.py bdist_wheel`
5. Install the "bigdataprojectlib" library from the wheel file located in the “dist” folder: `pip install /path/to/wheelfile.whl`
6. Download the AWS JARs into PySpark's JAR directory:

    ```shell
        cd .venv/Lib/site-packages/pyspark/jars || .venv/lib/python3.11/site-packages/pyspark/jars
        curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
        curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
    ```

7. Start the docker containers using the following command: `docker-compose up -d`
8. Verify that the containers are running. You should see Elasticsearch, Kibana, and LocalStack services running.

    ```shell
        docker ps
        localstack status services
    ```

    Here is the expected output:

    ```shell
        CONTAINER ID   IMAGE                                                 COMMAND                  CREATED       STATUS                 PORTS                                                                    NAMES
        33b5a195951e   confluentinc/cp-kafka:latest                          "/etc/confluent/dock…"   2 days ago   Up 21 seconds             9092/tcp, 0.0.0.0:29092->29092/tcp                                       kafka
        154f815874e6   docker.elastic.co/kibana/kibana:8.7.1                 "/bin/tini -- /usr/l…"   2 days ago   Up 58 minutes             0.0.0.0:5601->5601/tcp                                                   kibana
        cb2139aadafe   localstack/localstack:latest                          "docker-entrypoint.sh"   2 days ago   Up 58 minutes (healthy)   127.0.0.1:4510-4559->4510-4559/tcp, 127.0.0.1:4566->4566/tcp, 5678/tcp   localstack_main
        4f82345da2fa   confluentinc/cp-zookeeper:latest                      "/etc/confluent/dock…"   2 days ago   Up 58 minutes             2888/tcp, 3888/tcp, 0.0.0.0:22181->2181/tcp                              zookeeper
        9e9951a7be64   docker.elastic.co/elasticsearch/elasticsearch:8.7.1   "/bin/tini -- /usr/l…"   2 days ago   Up 58 minutes (healthy)   0.0.0.0:9200->9200/tcp, 9300/tcp                                         elasticsearch

        ┏━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
        ┃ Service                  ┃ Status      ┃
        ┡━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━┩
        │ acm                      │ ✔ available │
        │ apigateway               │ ✔ available │
        │ cloudformation           │ ✔ available │
        │ cloudwatch               │ ✔ available │
        │ config                   │ ✔ available │
        │ dynamodb                 │ ✔ available │
        │ dynamodbstreams          │ ✔ available │
        │ ec2                      │ ✔ available │
        │ es                       │ ✔ available │
        │ events                   │ ✔ available │
        │ firehose                 │ ✔ available │
        │ iam                      │ ✔ available │
        │ kinesis                  │ ✔ available │
        │ kms                      │ ✔ available │
        │ lambda                   │ ✔ available │
        │ logs                     │ ✔ available │
        │ opensearch               │ ✔ available │
        │ redshift                 │ ✔ available │
        │ resource-groups          │ ✔ available │
        │ resourcegroupstaggingapi │ ✔ available │
        │ route53                  │ ✔ available │
        │ route53resolver          │ ✔ available │
        │ s3                       │ ✔ running   │
        │ s3control                │ ✔ available │
        │ secretsmanager           │ ✔ available │
        │ ses                      │ ✔ available │
        │ sns                      │ ✔ available │
        │ sqs                      │ ✔ available │
        │ ssm                      │ ✔ available │
        │ stepfunctions            │ ✔ available │
        │ sts                      │ ✔ available │
        │ support                  │ ✔ available │
        │ swf                      │ ✔ available │
        │ transcribe               │ ✔ available │
        └──────────────────────────┴─────────────┘
    ```

9. For the S3 pipeline, create the S3 buckets for data ingestion, formatting, and combination:

    ```shell
        aws --endpoint-url=http://localhost:4566 s3 mb s3://big-data-project-ingestion
        aws --endpoint-url=http://localhost:4566 s3 mb s3://big-data-project-formatting
        aws --endpoint-url=http://localhost:4566 s3 mb s3://big-data-project-combination
    ```

10. For the Kafka pipeline, create the Kafka topics for each phase: ingestion, formatting, combination, and indexing.

    ```shell
        docker exec -it 33b5a195951e kafka-topics --create --topic ingest_prices_topic --bootstrap-server localhost:9092
        docker exec -it 33b5a195951e kafka-topics --create --topic ingest_news_topic --bootstrap-server localhost:9092
        docker exec -it 33b5a195951e kafka-topics --create --topic format_prices_topic --bootstrap-server localhost:9092
        docker exec -it 33b5a195951e kafka-topics --create --topic format_news_topic --bootstrap-server localhost:9092
        docker exec -it 33b5a195951e kafka-topics --create --topic combine_data_topic --bootstrap-server localhost:9092
    ```

11. Create the .env file from the example with the following command: `cp .env.example .env`
12. Run each script in the order of the pipeline: ingestion, formatting, combination, and indexing. Make sure to use the S3 versions for the S3 pipeline and the Kafka versions for the Kafka pipeline.

You can now explore the processed and combined data in Elasticsearch, and visualize it using Kibana!

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

[MIT](https://choosealicense.com/licenses/mit/)
