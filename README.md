# Big Data Project

## Description

This project aims to fetch stock market data, transform it using PySpark, and save the transformed data into a Parquet file in an Amazon S3 bucket.

## Getting Started

1. Clone the repository to your local machine.
2. Create a virtual environment and activate it: `python3 -m venv venv && source venv/bin/activate`
3. Install the required packages using the following command: `pip install -r requirements.txt`
4. Download the AWS JARs into PySpark's JAR directory:

```shell
cd .venv/Lib/site-packages/pyspark/jars || .venv/lib/python3.11/site-packages/pyspark/jars
ls -l | grep hadoop
curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
```

5. Start the docker containers using the following command: `docker-compose up -d`
6. Verify that the containers are running using the following command:

```shell
docker ps
localstack status services
```
