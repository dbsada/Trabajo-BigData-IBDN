import os

def s3a_flags(endpoint=None, access_key=None, secret_key=None):
    endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = access_key or os.getenv("MINIO_ROOT_USER", "admin")
    secret_key = secret_key or os.getenv("MINIO_ROOT_PASSWORD", "password")
    return (
        f"--conf spark.hadoop.fs.s3a.access.key={access_key} "
        f"--conf spark.hadoop.fs.s3a.secret.key={secret_key} "
        f"--conf spark.hadoop.fs.s3a.endpoint={endpoint} "
        f"--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
        f"--conf spark.hadoop.fs.s3a.path.style.access=true "
        f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false"
    )

def spark_submit(job_class, jar=None, master=None, deploy_mode="cluster", cores=2, extra_confs=None, extra_java_opts=None):
    master = master or os.getenv("SPARK_MASTER_URL", "spark://spark-manager:7077")
    jar = jar or os.getenv("PREDICTION_JAR", "/app/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar")
    cmd = (
        f"spark-submit --master {master} "
        f"--deploy-mode {deploy_mode} "
        f"--conf spark.cores.max={cores} "
        f"{s3a_flags()}"
    )
    if extra_java_opts:
        cmd += f' --conf spark.driver.extraJavaOptions={extra_java_opts}'
    if extra_confs:
        for conf in extra_confs:
            cmd += f" --conf {conf}"
    cmd += f" --class {job_class} {jar}"
    return cmd

def spark_submit_train(extra_args=None):
    extra_confs = [
        "spark.driver.extraJavaOptions=--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "spark.executor.memory=3g",
    ]
    cmd = spark_submit(
        job_class="es.upm.dit.ging.predictor.TrainModel",
        extra_confs=extra_confs,
    )
    if extra_args:
        cmd += " " + extra_args
    return cmd

def spark_submit_predict():
    return spark_submit(job_class="es.upm.dit.ging.predictor.MakePrediction")