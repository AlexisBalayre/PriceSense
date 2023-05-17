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
Here is the expected output:

```shell
CONTAINER ID   IMAGE                                                 COMMAND                  CREATED       STATUS                 PORTS                                                                    NAMES
83591d116178   docker.elastic.co/kibana/kibana:8.7.1                 "/bin/tini -- /usr/l…"   2 hours ago   Up 2 hours             0.0.0.0:5601->5601/tcp                                                   kibana
d71ae92cc74f   localstack/localstack:latest                          "docker-entrypoint.sh"   2 hours ago   Up 2 hours (healthy)   127.0.0.1:4510-4559->4510-4559/tcp, 127.0.0.1:4566->4566/tcp, 5678/tcp   localstack_main
6a4e5c245320   docker.elastic.co/elasticsearch/elasticsearch:8.7.1   "/bin/tini -- /usr/l…"   2 hours ago   Up 2 hours (healthy)   0.0.0.0:9200->9200/tcp, 9300/tcp  

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
└──────────────────────────┴─────────────┘                                    elasticsearch
```


