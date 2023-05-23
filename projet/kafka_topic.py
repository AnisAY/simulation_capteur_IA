from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import datetime
from kafka import *
from minio import Minio
from minio.error import S3Error
import datetime
import json
import random

def main():
    # Connect to Kafka producer
    producer = KafkaProducer(bootstrap_servers='localhost:2909')

    # Connect to local Spark instance
    #spark = SparkSession.builder.appName("CapteursDataStorage").master("spark://spark-master:7077").getOrCreate()

    spark = (SparkSession.builder.appName("CapteursDataStorage")
           .master("spark://spark-master:7077")
           .getOrCreate())

    # Define schema for Capteurs data
    # ["timestamp", "entrance_amount", "exit_amount", "temperature", "humidity", "parking_entrance", "parking_exit", "parking_actual_vehicle"]
    schema = StructType([
      StructField("timestamp", TimestampType(), True),
      StructField("temperature", DoubleType(), True),
      StructField("humidity", DoubleType(), True),
      StructField("entrance_amount", IntegerType(), True),
      StructField("exit_amount", IntegerType(), True),
      StructField("parking_entrance", IntegerType(), True),
      StructField("parking_exit", IntegerType(), True),
      StructField("parking_actual_vehicle", IntegerType(), True),
    ])

    # Define Kafka topic to read from
    topic = "capteur"

    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:2909") \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

    query = df \
    .writeStream \
    .format("parquet") \
    .option("path", "s3a://donnees-kafka/processed_data") \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .start()

if __name__ == "__main__":
    main()