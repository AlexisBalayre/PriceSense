# run Localstack
# docker-compose up -d

# run spark in local mode
export SPARK_LOCAL_IP=127.0.0.1
spark-submit \
  --packages commons-logging:commons-logging:1.2,software.amazon.awssdk:s3:2.20.66,org.apache.hadoop:hadoop-aws:3.3.5 \
  --conf spark.hadoop.fs.s3a.endpoint=http://localhost:4566 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.access.key=test \
  --conf spark.hadoop.fs.s3a.secret.key=test \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  spark-s3-test.py