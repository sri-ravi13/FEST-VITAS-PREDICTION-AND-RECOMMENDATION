import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_date, datediff, date_format, when, count
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import DoubleType
import sys
import os
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
jar_filenames = [
    "mongo-spark-connector_2.12-3.0.2.jar",
    "mongo-java-driver-3.12.11.jar",
    "bson-4.0.5.jar"
]
# Point this to your actual Spark Jars directory
spark_jars_path = r"C:\spark\spark-3.5.6-bin-hadoop3\jars"
jar_paths = [os.path.join(spark_jars_path, filename) for filename in jar_filenames]
jars_string = ",".join(jar_paths)

MONGO_SPARK_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {MONGO_SPARK_PACKAGE} pyspark-shell'
MONGO_URI = "mongodb://127.0.0.1:27017"
MONGO_DB = "event_pulse_db"
MONGO_COLLECTION = "events"

print("Configuring Spark Session for Demand Forecasting Model...")
spark = SparkSession.builder \
    .appName("TrainDemandForecastingModel") \
    .master("local[*]") \
    .config("spark.jars", jars_string) \
    .config("spark.mongodb.input.uri", f"{MONGO_URI}/{MONGO_DB}.{MONGO_COLLECTION}") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print(f"--- Loading data from MongoDB collection: {MONGO_COLLECTION} ---")
df = spark.read.format("mongo").load().na.drop(subset=["id", "name", "avg_price"])

if df.count() < 50:
    print("Insufficient data to train model. Exiting.")
    spark.stop()
    exit()
print(f"Successfully loaded {df.count()} events.")

# --- 2. Feature and Label Engineering ---
print("--- Starting feature and label engineering ---")
# a. Standard Feature Engineering
name_counts = df.groupBy("name").agg(count("id").alias("event_name_count"))
df_featured = df.join(name_counts, on="name", how="left").na.fill(1, ["event_name_count"])
df_featured = df_featured.withColumn("event_date_dt", to_date(col("event_date"), "yyyy-MM-dd")) \
                         .withColumn("days_until_event", datediff(col("event_date_dt"), current_date())) \
                         .withColumn("is_weekend", when(date_format(col("event_date_dt"), "E").isin(["Sat", "Sun"]), 1).otherwise(0))

vec_assembler_for_scaling = VectorAssembler(inputCols=["avg_price", "event_name_count"], outputCol="scaling_features")
df_vectorized = vec_assembler_for_scaling.transform(df_featured)

# Use MinMaxScaler to scale them to a 0-1 range
scaler = MinMaxScaler(inputCol="scaling_features", outputCol="scaled_features")
scaler_model = scaler.fit(df_vectorized)
df_scaled = scaler_model.transform(df_vectorized)

# Define a UDF to combine the scaled features and create the final score
def calculate_demand_score(vector):
    score_0_to_1 = (vector[0] * 0.5) + (vector[1] * 0.5)
    # Scale to 1-100 for user readability
    return float(1 + (score_0_to_1 * 99))

demand_score_udf = spark.udf.register("demand_score_udf", calculate_demand_score)


# Apply the UDF to create the final "label" column
df_final = df_scaled.withColumn("label", demand_score_udf(col("scaled_features")).cast(DoubleType()))
print("Created demand score label (1-100).")


categorical_cols = ["segment", "genre", "venue_name", "city", "state"]
indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid="keep") for c in categorical_cols]
encoder = OneHotEncoder(inputCols=[f"{c}_index" for c in categorical_cols], outputCols=[f"{c}_vec" for c in categorical_cols])

# Assembler for all final features
feature_cols = ["days_until_event", "is_weekend"] + [f"{c}_vec" for c in categorical_cols]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")

# Define the Regressor (instead of a Classifier)
gbt = GBTRegressor(labelCol="label", featuresCol="features")

pipeline = Pipeline(stages=indexers + [encoder, assembler, gbt])

# --- 4. Train and Evaluate the Model ---
(trainingData, testData) = df_final.randomSplit([0.8, 0.2], seed=42)
print(f"\n--- Training the demand forecasting model... ---")
pipeline_model = pipeline.fit(trainingData)

print("--- Making predictions on the test data... ---")
predictions = pipeline_model.transform(testData)

# Use RegressionEvaluator for evaluation
evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")

rmse = evaluator_rmse.evaluate(predictions)
r2 = evaluator_r2.evaluate(predictions)

print(f"\nModel Performance on Test Data:")
print(f"Root Mean Squared Error (RMSE) = {rmse:.2f}")
print(f"R2 Score = {r2:.2f}")

# --- 5. Save the New Model ---
model_path = "models/event_demand_model"
pipeline_model.write().overwrite().save(model_path)
print(f"\n--- Demand forecasting model successfully saved to: {model_path} ---")

spark.stop()