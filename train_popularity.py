import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, to_date, current_date, datediff, date_format, when, count
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
jar_filename = "mongo-spark-connector_2.12-10.1.0-all.jar"
spark_jars_path_str = r"C:\spark\spark-3.5.6-bin-hadoop3\jars"
full_jar_path = os.path.join(spark_jars_path_str, jar_filename)
if not os.path.exists(full_jar_path):
    print("=" * 60)
    print(f"FATAL ERROR: The specified JAR file was not found!")
    print(f"Path: {full_jar_path}")
    print("Please make sure the filename is correct and the file is in the folder.")
    print("=" * 60)
    exit()
jars_uri_string = "file:///" + full_jar_path.replace("\\", "/")
MONGO_SPARK_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {MONGO_SPARK_PACKAGE} pyspark-shell'
MONGO_URI = "mongodb://127.0.0.1:27017"
MONGO_DB = "event_pulse_db"
MONGO_COLLECTION = "events"

print("Configuring Spark Session with local MongoDB 10.1.0 JAR...")
spark = SparkSession.builder \
    .appName("TrainAdvancedPopularityModel") \
    .master("local[*]") \
    .config("spark.jars", jars_uri_string) \
    .config("spark.mongodb.input.uri", f"{MONGO_URI}/{MONGO_DB}.{MONGO_COLLECTION}") \
    .getOrCreate()

print("Spark Session created successfully!")
spark.sparkContext.setLogLevel("WARN")
print(f"--- Loading data from MongoDB collection: {MONGO_COLLECTION} ---")
df = spark.read.format("mongo").load()

if "_id" in df.columns:
    df = df.drop("_id")

if df.isEmpty():
    print("DataFrame is empty after loading from MongoDB. Check your database and collection names.")
    spark.stop()
    exit()

df.cache()

if df.count() < 50:
    print(f"Database has insufficient data ({df.count()} records). Cannot train model. Exiting.")
    spark.stop()
    exit()

print(f"Successfully loaded {df.count()} events from the database.")
print("--- Starting advanced feature engineering ---")

name_counts = df.groupBy("name").agg(count("id").alias("event_name_count"))
df_featured = df.join(name_counts, on="name", how="left").na.fill(1, ["event_name_count"])

df_featured = df_featured.withColumn("event_date_dt", to_date(col("event_date"), "yyyy-MM-dd")) \
                         .withColumn("days_until_event", datediff(col("event_date_dt"), current_date())) \
                         .withColumn("day_of_week", date_format(col("event_date_dt"), "E")) \
                         .withColumn("is_weekend", when(col("day_of_week").isin(["Sat", "Sun"]), 1).otherwise(0))

price_df = df_featured.filter(col("avg_price").isNotNull())
if price_df.count() == 0:
    print("No price data available to define a label. Exiting.")
    spark.stop()
    exit()
price_threshold = price_df.approxQuantile("avg_price", [0.7], 0.01)[0]
df_featured = df_featured.withColumn("label", when(col("avg_price") >= price_threshold, 1).otherwise(0))
print(f"Defined high-risk label for events with avg_price >= ${price_threshold:.2f}")
segment_indexer = StringIndexer(inputCol="segment", outputCol="segment_index", handleInvalid="keep")
genre_indexer = StringIndexer(inputCol="genre", outputCol="genre_index", handleInvalid="keep")
venue_indexer = StringIndexer(inputCol="venue_name", outputCol="venue_index", handleInvalid="keep")
city_indexer = StringIndexer(inputCol="city", outputCol="city_index", handleInvalid="keep")
state_indexer = StringIndexer(inputCol="state", outputCol="state_index", handleInvalid="keep")

encoder = OneHotEncoder(
    inputCols=["segment_index", "genre_index", "venue_index", "city_index", "state_index"],
    outputCols=["segment_vec", "genre_vec", "venue_vec", "city_vec", "state_vec"]
)

feature_cols = [
    "days_until_event", "is_weekend", "event_name_count",
    "segment_vec", "genre_vec", "venue_vec", "city_vec", "state_vec"
]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")

gbt = GBTClassifier(labelCol="label", featuresCol="features")

pipeline = Pipeline(stages=[
    segment_indexer, genre_indexer, venue_indexer, city_indexer, state_indexer,
    encoder,
    assembler,
    gbt
])
print("--- Balancing dataset with oversampling ---")
minority_df = df_featured.filter(col("label") == 1)
majority_df = df_featured.filter(col("label") == 0)

if minority_df.count() == 0 or majority_df.count() == 0:
    print("Dataset contains only one class. Cannot train a classifier. Exiting.")
    spark.stop()
    exit()

if minority_df.count() > 0:
    ratio = float(majority_df.count()) / float(minority_df.count())
    oversampled_minority_df = minority_df.sample(withReplacement=True, fraction=ratio, seed=1234)
    balanced_df = majority_df.unionAll(oversampled_minority_df)
else:
    balanced_df = majority_df

print(f"Dataset balanced. Original minority count: {minority_df.count()}, New count: {balanced_df.filter(col('label') == 1).count()}")
(trainingData, testData) = balanced_df.randomSplit([0.8, 0.2], seed=42)
print(f"Training data count: {trainingData.count()}, Test data count: {testData.count()}")

if testData.count() > 0:
    print("\n--- Training the pipeline model... ---")
    pipeline_model = pipeline.fit(trainingData)
    
    print("--- Making predictions on the test data... ---")
    predictions = pipeline_model.transform(testData)
    
    accuracyEvaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = accuracyEvaluator.evaluate(predictions)
    print(f"\nModel Accuracy on Test Data = {accuracy * 100:.2f}%")

    aucEvaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = aucEvaluator.evaluate(predictions)
    print(f"Area Under ROC Curve (AUC) = {auc:.2f}")

    model_path = "models/event_popularity_model"
    pipeline_model.write().overwrite().save(model_path)
    print(f"\n--- PipelineModel successfully saved to: {model_path} ---")
else:
    print("\nWarning: Test dataset is empty. Cannot evaluate or save the model.")

spark.stop()