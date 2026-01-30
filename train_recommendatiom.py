import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, collect_list, struct, slice
from pyspark.ml.feature import HashingTF, IDF, Normalizer, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.feature import BucketedRandomProjectionLSH

jar_filenames = [
    "mongo-spark-connector_2.12-3.0.2.jar",
    "mongo-java-driver-3.12.11.jar",
    "bson-4.0.5.jar"
]

spark_jars_path = r"C:\spark\spark-3.5.6-bin-hadoop3\jars"
jar_paths = [os.path.join(spark_jars_path, filename) for filename in jar_filenames]
jars_string = ",".join(jar_paths)
MONGO_SPARK_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {MONGO_SPARK_PACKAGE} pyspark-shell'
MONGO_URI = "mongodb://127.0.0.1:27017"
MONGO_DB = "event_pulse_db"
MONGO_COLLECTION = "events"

print("Configuring Spark Session with local MongoDB 3.0.2 JARs...")
spark = SparkSession.builder \
    .appName("TrainRecommenderModel") \
    .master("local[*]") \
    .config("spark.jars", jars_string) \
    .config("spark.mongodb.input.uri", f"{MONGO_URI}/{MONGO_DB}.{MONGO_COLLECTION}") \
    .getOrCreate()

print("Spark Session created successfully!")
spark.sparkContext.setLogLevel("WARN")
print(f"--- Loading data from MongoDB collection: {MONGO_COLLECTION} ---")
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load() \
    .select("id", "name", "segment", "genre", "venue_name", "city", "state").na.drop()
df_profile = df.withColumn("profile", concat_ws(" ", col("segment"), col("genre"), col("venue_name"), col("city"), col("state")))
print("Created content profiles for events.")
tokenizer = Tokenizer(inputCol="profile", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=2**12)
idf = IDF(inputCol="rawFeatures", outputCol="idf_features")
normalizer = Normalizer(inputCol="idf_features", outputCol="features")

pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, normalizer])
model = pipeline.fit(df_profile)
vectorized_df = model.transform(df_profile)
print("Vectorized event profiles using TF-IDF.")

brp = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes", bucketLength=2.0, numHashTables=5)
lsh_model = brp.fit(vectorized_df)
print("Finding top 10 similar events for each event using LSH...")
similar_items = lsh_model.approxSimilarityJoin(vectorized_df, vectorized_df, 1.0, "distance") \
    .filter("datasetA.id != datasetB.id") \
    .select(
        col("datasetA.id").alias("id"),
        col("datasetA.name").alias("name"),
        col("datasetB.id").alias("similar_id"),
        col("datasetB.name").alias("similar_name"),
        col("distance")
    ).orderBy("id", "distance")
recommendations = similar_items.groupBy("id", "name").agg(
    collect_list(
        struct(col("similar_id"), col("similar_name"))
    ).alias("recommendations_list")
)
recommendations = recommendations.withColumn("recommendations", slice(col("recommendations_list"), 1, 10)) \
                                 .select("id", "name", "recommendations")
output_path = "models/event_recommender_data"
recommendations.write.mode("overwrite").parquet(output_path)
print(f"\n--- Recommendation data successfully saved to: {output_path} ---")

spark.stop()