from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder \
    .appName("YouTube Video ETag and Names Batch Processing") \
    .getOrCreate()

etag_name_json_path = "video_names_and_etags.json"
s3_path_etag_name = "s3a://wonseokchoi-data-lake-project/batch_layer/video_etag_name/"
delta_lake_path_etag_name = "s3a://wonseokchoi-data-lake-project/delta_lake/batch_layer/video_etag_name/"

etag_name_df = spark.read.json(etag_name_json_path)
etag_name_df = etag_name_df.withColumnRenamed("_1", "etag").withColumnRenamed("_2", "name")

etag_name_df.write.parquet(s3_path_etag_name, mode="overwrite")

etag_name_df.write.format("delta").mode("overwrite").save(delta_lake_path_etag_name)

spark.stop()
