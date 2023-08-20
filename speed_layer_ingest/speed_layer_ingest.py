from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

spark = SparkSession.builder \
    .appName("YouTube Metrics Consumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,io.delta:delta-core_2.12:1.0.0") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .getOrCreate()

kafka_topic = "youtube-metrics"
kafka_bootstrap_servers = "localhost:9092" 

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

s3_path = "s3a://wonseokchoi-data-lake-project/speed_layer" 
df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", s3_path) \
    .option("checkpointLocation", s3_path + "/checkpoints") \
    .start()

delta_lake_path = "s3a://wonseokchoi-data-lake-project/delta_lake/speed_layer"
df.writeStream \
    .format("delta") \
    .option("path", delta_lake_path) \
    .option("checkpointLocation", delta_lake_path + "/checkpoints") \
    .start()

spark.streams.awaitAnyTermination()
