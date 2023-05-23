from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *




def main():
  # Connect to Kafka producer
  producer = KafkaProducer(bootstrap_servers='localhost:9092')

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
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

  # # Read data from Kafka topic
  # df = spark \
  #     .readStream \
  #     .format("kafka") \
  #     .option("kafka.bootstrap.servers", "localhost:9092") \
  #     .option("subscribe", topic) \
  #     .load()

# query = df \
#    .writeStream \
#    .format("parquet") \
#    .option("path", "capteurs_data_storage") \
#    .option("checkpointLocation", "capteurs_data_storage_checkpoint") \
#    .start()
if __name__ == "__main__":
  main()