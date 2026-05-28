import sys, os, re, argparse, logging, time
from os import environ

import mlflow

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def main(base_path=None):

  parser = argparse.ArgumentParser()
  parser.add_argument('--max-bins', type=int, default=4657)
  parser.add_argument('--max-memory-mb', type=int, default=1024)
  parser.add_argument('--num-trees', type=int, default=20)
  parser.add_argument('--max-depth', type=int, default=10)
  parser.add_argument('--run-name', type=str, default=None)
  args, _ = parser.parse_known_args()

  APP_NAME = "train_spark_mllib_model.py"
  
  from pyspark.sql import SparkSession

  spark = SparkSession.builder \
    .appName(APP_NAME) \
    .getOrCreate()
  
  #
  # {
  #   "ArrDelay":5.0,"CRSArrTime":"2015-12-31T03:20:00.000-08:00","CRSDepTime":"2015-12-31T03:05:00.000-08:00",
  #   "Carrier":"WN","DayOfMonth":31,"DayOfWeek":4,"DayOfYear":365,"DepDelay":14.0,"Dest":"SAN","Distance":368.0,
  #   "FlightDate":"2015-12-30T16:00:00.000-08:00","FlightNum":"6109","Origin":"TUS"
  # }
  #
  from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, DateType, TimestampType
  from pyspark.sql.types import StructType, StructField
  from pyspark.sql.functions import udf
  
  schema = StructType([
    StructField("ArrDelay", DoubleType(), True),     # "ArrDelay":5.0
    StructField("CRSArrTime", TimestampType(), True),    # "CRSArrTime":"2015-12-31T03:20:00.000-08:00"
    StructField("CRSDepTime", TimestampType(), True),    # "CRSDepTime":"2015-12-31T03:05:00.000-08:00"
    StructField("Carrier", StringType(), True),     # "Carrier":"WN"
    StructField("DayOfMonth", IntegerType(), True), # "DayOfMonth":31
    StructField("DayOfWeek", IntegerType(), True),  # "DayOfWeek":4
    StructField("DayOfYear", IntegerType(), True),  # "DayOfYear":365
    StructField("DepDelay", DoubleType(), True),     # "DepDelay":14.0
    StructField("Dest", StringType(), True),        # "Dest":"SAN"
    StructField("Distance", DoubleType(), True),     # "Distance":368.0
    StructField("FlightDate", DateType(), True),    # "FlightDate":"2015-12-30T16:00:00.000-08:00"
    StructField("FlightNum", StringType(), True),   # "FlightNum":"6109"
    StructField("Origin", StringType(), True),      # "Origin":"TUS"
  ])
  
  input_path = "s3a://lakehouse/raw/simple_flight_delay_features.jsonl.bz2"
  features = spark.read.json(input_path, schema=schema).repartition(4)
  features.first()

  features.writeTo("lakehouse.flight_delays").createOrReplace()
  logging.info("✅ Tabla Iceberg 'lakehouse.flight_delays' creada/actualizada")

  features = spark.table("lakehouse.flight_delays")
  
  #
  # Check for nulls in features before using Spark ML
  #
  null_counts = [(column, features.where(features[column].isNull()).count()) for column in features.columns]
  cols_with_nulls = filter(lambda x: x[1] > 0, null_counts)
  print(list(cols_with_nulls))
  
  #
  # Add a Route variable to replace FlightNum
  #
  from pyspark.sql.functions import lit, concat
  features_with_route = features.withColumn(
    'Route',
    concat(
      features.Origin,
      lit('-'),
      features.Dest
    )
  )
  features_with_route.show(6)
  
  #
  # Use pysmark.ml.feature.Bucketizer to bucketize ArrDelay into on-time, slightly late, very late (0, 1, 2)
  #
  from pyspark.ml.feature import Bucketizer
  
  # Setup the Bucketizer
  splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]
  arrival_bucketizer = Bucketizer(
    splits=splits,
    inputCol="ArrDelay",
    outputCol="ArrDelayBucket"
  )
  
  arrival_bucketizer_path = "s3a://lakehouse/models/arrival_bucketizer_2.0.bin"
  arrival_bucketizer.write().overwrite().save(arrival_bucketizer_path)
  logging.info(f"✅ Bucketizer guardado en {arrival_bucketizer_path}")
  
  # Apply the bucketizer
  ml_bucketized_features = arrival_bucketizer.transform(features_with_route)
  ml_bucketized_features.select("ArrDelay", "ArrDelayBucket").show()
  
  #
  # Extract features tools in with pyspark.ml.feature
  #
  from pyspark.ml.feature import StringIndexer, VectorAssembler
  
  # Turn category fields into indexes
  for column in ["Carrier", "Origin", "Dest", "Route"]:
    string_indexer = StringIndexer(
      inputCol=column,
      outputCol=column + "_index"
    )
    
    string_indexer_model = string_indexer.fit(ml_bucketized_features)
    ml_bucketized_features = string_indexer_model.transform(ml_bucketized_features)
    
    # Drop the original column
    ml_bucketized_features = ml_bucketized_features.drop(column)
    
    string_indexer_output_path = "s3a://lakehouse/models/string_indexer_model_{}.bin".format(column)
    string_indexer_model.write().overwrite().save(string_indexer_output_path)
    logging.info(f"✅ StringIndexer para {column} guardado en {string_indexer_output_path}")
  
  # Combine continuous, numeric fields with indexes of nominal ones
  # ...into one feature vector
  numeric_columns = [
    "DepDelay", "Distance",
    "DayOfMonth", "DayOfWeek",
    "DayOfYear"]
  index_columns = ["Carrier_index", "Origin_index",
                   "Dest_index", "Route_index"]
  vector_assembler = VectorAssembler(
    inputCols=numeric_columns + index_columns,
    outputCol="Features_vec"
  )
  final_vectorized_features = vector_assembler.transform(ml_bucketized_features)
  
  vector_assembler_path = "s3a://lakehouse/models/numeric_vector_assembler.bin"
  vector_assembler.write().overwrite().save(vector_assembler_path)
  logging.info(f"✅ VectorAssembler guardado en {vector_assembler_path}")
  
  # Drop the index columns
  for column in index_columns:
    final_vectorized_features = final_vectorized_features.drop(column)
  
  # Inspect the finalized features
  final_vectorized_features.show()
  
  # Instantiate, fit and evaluate random forest classifier
  from pyspark.ml.classification import RandomForestClassifier
  from pyspark.ml.evaluation import MulticlassClassificationEvaluator

  mlflow.set_tracking_uri(environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))

  run_name = args.run_name or "rf_training"
  with mlflow.start_run(run_name=run_name) as run:
    _t0 = time.time()
    rfc = RandomForestClassifier(
      featuresCol="Features_vec",
      labelCol="ArrDelayBucket",
      predictionCol="Prediction",
      maxBins=args.max_bins,
      maxMemoryInMB=args.max_memory_mb,
      numTrees=args.num_trees,
      maxDepth=args.max_depth
    )

    mlflow.log_param("maxBins", args.max_bins)
    mlflow.log_param("maxMemoryInMB", args.max_memory_mb)
    mlflow.log_param("numTrees", args.num_trees)
    mlflow.log_param("maxDepth", args.max_depth)
    mlflow.log_param("model_version", environ.get("MODEL_VERSION", "5.0"))

    model = rfc.fit(final_vectorized_features)

    model_output_path = "s3a://lakehouse/models/spark_random_forest_classifier.flight_delays.5.0.bin"
    model.write().overwrite().save(model_output_path)
    logging.info(f"✅ RandomForest model guardado en {model_output_path}")

    predictions = model.transform(final_vectorized_features)

    evaluator = MulticlassClassificationEvaluator(
      predictionCol="Prediction",
      labelCol="ArrDelayBucket",
      metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)
    mlflow.log_metric("accuracy", accuracy)
    print("Accuracy = {}".format(accuracy))

    # Check the distribution of predictions
    predictions.groupBy("Prediction").count().show()

    # Check a sample
    predictions.sample(False, 0.001, 18).orderBy("CRSDepTime").show(6)

    duration = round(time.time() - _t0)
    mlflow.log_param("training_duration_seconds", duration)
    print(f"Training completed in {duration}s")

    try:
      mlflow.spark.log_model(model, "model")
    except Exception as e:
      print(f"MLflow log_model error (non-fatal): {e}")

if __name__ == "__main__":
  main()
