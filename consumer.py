
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pymongo import MongoClient
import os

MONGO_SPARK_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1"
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {MONGO_SPARK_PACKAGE} pyspark-shell'

KAFKA_JARS = [
    "file:///C:/spark/spark-3.5.6-bin-hadoop3/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "file:///C:/spark/spark-3.5.6-bin-hadoop3/jars/kafka-clients-3.4.1.jar",
    "file:///C:/spark/spark-3.5.6-bin-hadoop3/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    "file:///C:/spark/spark-3.5.6-bin-hadoop3/jars/commons-pool2-2.11.1.jar"
]
JARS_STRING = ",".join(KAFKA_JARS)

mongo_client = MongoClient("mongodb://127.0.0.1:27017/")
mongo_db = mongo_client["event_pulse_db"]
mongo_collection = mongo_db["events"]

print("Configuring Spark Session with Kafka JARs and MongoDB Package...")
spark = SparkSession.builder \
    .appName("KafkaToMongoConsumer") \
    .config("spark.jars.packages", 
          "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
          "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1")\
    .master("local[*]") \
    .config("spark.jars", JARS_STRING) \
    .config("spark.mongodb.output.uri", f"{mongo_client}.{mongo_collection}") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session created successfully.")



KAFKA_TOPIC = "ticketmaster_events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("url", StringType(), True),
    StructField("event_date", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("min_price", DoubleType(), True),
    StructField("max_price", DoubleType(), True),
    StructField("avg_price", DoubleType(), True),
    StructField("ingestion_timestamp", StringType(), True)
])


print(f"Subscribing to Kafka topic: {KAFKA_TOPIC}")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")


def write_batch_to_mongodb(df, epoch_id):



    if not df.rdd.isEmpty():
        print(f"--- Writing batch {epoch_id} to MongoDB ({df.count()} records) ---")
        records = df.toPandas().to_dict("records")
        try:
            mongo_collection.insert_many(records, ordered=False) # 'ordered=False' can be faster
            print("--- Batch written successfully ---")
        except Exception as e:
            print(f"Error writing batch to MongoDB: {e}")

query = parsed_df.writeStream \
    .foreachBatch(write_batch_to_mongodb) \
    .outputMode("update") \
    .start()

print("Streaming query started. Writing data from Kafka to MongoDB...")
query.awaitTermination()