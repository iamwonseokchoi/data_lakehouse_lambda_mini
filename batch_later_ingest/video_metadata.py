from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("YouTube Video Metadata Batch Processing") \
    .getOrCreate()

metadata_json_path = "video_metadata.json"
s3_path_metadata = "s3a://wonseokchoi-data-lake-project/batch_layer/video_metadata/"
delta_lake_path_metadata = "s3a://wonseokchoi-data-lake-project/delta_lake/batch_layer/video_metadata/"

metadata_df = spark.read.json(metadata_json_path)

metadata_df.write.parquet(s3_path_metadata, mode="overwrite")

metadata_df.write.format("delta").mode("overwrite").save(delta_lake_path_metadata)

spark.stop()
