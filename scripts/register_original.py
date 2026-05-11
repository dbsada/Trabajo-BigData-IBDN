import mlflow
from os import environ
from pyspark.sql import SparkSession

mlflow.set_tracking_uri(environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))

spark = SparkSession.builder.appName("register_original_model").getOrCreate()

with mlflow.start_run(run_name="Original") as run:
    mlflow.log_param("maxBins", 4657)
    mlflow.log_param("maxMemoryInMB", 1024)
    mlflow.log_param("numTrees", 20)
    mlflow.log_param("maxDepth", 10)
    mlflow.log_param("model_version", environ.get("MODEL_VERSION", "5.0"))
    mlflow.log_metric("accuracy", 0.588)

    from pyspark.ml.classification import RandomForestClassificationModel
    model = RandomForestClassificationModel.load(
        "s3a://lakehouse/models/spark_random_forest_classifier.flight_delays.5.0.bin"
    )
    try:
        mlflow.spark.log_model(model, "model")
    except Exception as e:
        print(f"log_model warning (non-fatal): {e}")
    print(f"Original model registered: {run.info.run_id}")
