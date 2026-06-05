package es.upm.dit.ging.predictor

import org.apache.spark.ml.classification.{RandomForestClassifier, RandomForestClassificationModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{Bucketizer, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{concat, lit}

object TrainModel {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()

    var maxBins = 4657
    var maxMemoryMB = 1024
    var numTrees = 20
    var maxDepth = 10
    var runName = "rf_training"
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--max-bins" => maxBins = args(i + 1).toInt; i += 2
        case "--max-memory-mb" => maxMemoryMB = args(i + 1).toInt; i += 2
        case "--num-trees" => numTrees = args(i + 1).toInt; i += 2
        case "--max-depth" => maxDepth = args(i + 1).toInt; i += 2
        case "--run-name" => runName = args(i + 1); i += 2
        case _ => i += 1
      }
    }

    println(s"Training with: maxBins=$maxBins maxMemoryMB=$maxMemoryMB numTrees=$numTrees maxDepth=$maxDepth")

    val spark = SparkSession.builder
      .appName("train_spark_mllib_model")
      .getOrCreate()
    import spark.implicits._

    val schema = StructType(Seq(
      StructField("ArrDelay", DoubleType, nullable = true),
      StructField("CRSArrTime", TimestampType, nullable = true),
      StructField("CRSDepTime", TimestampType, nullable = true),
      StructField("Carrier", StringType, nullable = true),
      StructField("DayOfMonth", IntegerType, nullable = true),
      StructField("DayOfWeek", IntegerType, nullable = true),
      StructField("DayOfYear", IntegerType, nullable = true),
      StructField("DepDelay", DoubleType, nullable = true),
      StructField("Dest", StringType, nullable = true),
      StructField("Distance", DoubleType, nullable = true),
      StructField("FlightDate", DateType, nullable = true),
      StructField("FlightNum", StringType, nullable = true),
      StructField("Origin", StringType, nullable = true),
    ))

    val inputPath = "s3a://lakehouse/raw/simple_flight_delay_features.jsonl.bz2"
    var features = spark.read.schema(schema).json(inputPath).repartition(4)
    features.first()

    features.writeTo("lakehouse.flight_delays").createOrReplace()
    println("Table created/updated")

    features = spark.table("lakehouse.flight_delays")

    val featuresWithRoute = features.withColumn("Route", concat($"Origin", lit("-"), $"Dest"))
    featuresWithRoute.show(6)

    val splits = Array(Double.NegativeInfinity, -15.0, 0, 30.0, Double.PositiveInfinity)
    val bucketizer = new Bucketizer()
      .setSplits(splits)
      .setInputCol("ArrDelay")
      .setOutputCol("ArrDelayBucket")

    val bucketizerPath = "s3a://lakehouse/models/arrival_bucketizer_2.0.bin"
    bucketizer.write.overwrite.save(bucketizerPath)
    println(s"Bucketizer saved to $bucketizerPath")

    var mlFeatures = bucketizer.transform(featuresWithRoute)
    mlFeatures.select("ArrDelay", "ArrDelayBucket").show()

    val stringColumns = Seq("Carrier", "Origin", "Dest", "Route")
    for (col <- stringColumns) {
      val indexer = new StringIndexer()
        .setInputCol(col)
        .setOutputCol(col + "_index")
      val model = indexer.fit(mlFeatures)
      mlFeatures = model.transform(mlFeatures).drop(col)
      val path = s"s3a://lakehouse/models/string_indexer_model_$col.bin"
      model.write.overwrite.save(path)
      println(s"StringIndexer for $col saved")
    }

    val numericCols = Array("DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear")
    val indexCols = stringColumns.map(_ + "_index").toArray
    val assembler = new VectorAssembler()
      .setInputCols(numericCols ++ indexCols)
      .setOutputCol("Features_vec")

    val finalVectorized = assembler.transform(mlFeatures)
    val assemblerPath = "s3a://lakehouse/models/numeric_vector_assembler.bin"
    assembler.write.overwrite.save(assemblerPath)
    println(s"VectorAssembler saved to $assemblerPath")

    for (col <- indexCols) finalVectorized.drop(col)
    finalVectorized.show()

    val rfc = new RandomForestClassifier()
      .setFeaturesCol("Features_vec")
      .setLabelCol("ArrDelayBucket")
      .setPredictionCol("Prediction")
      .setMaxBins(maxBins)
      .setMaxMemoryInMB(maxMemoryMB)
      .setNumTrees(numTrees)
      .setMaxDepth(maxDepth)

    val model = rfc.fit(finalVectorized)

    val modelPath = "s3a://lakehouse/models/spark_random_forest_classifier.flight_delays.5.0.bin"
    model.write.overwrite.save(modelPath)
    println(s"Model saved to $modelPath")

    val predictions = model.transform(finalVectorized)
    val evaluator = new MulticlassClassificationEvaluator()
      .setPredictionCol("Prediction")
      .setLabelCol("ArrDelayBucket")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println(s"Accuracy = $accuracy")

    predictions.groupBy("Prediction").count.show()
    predictions.sample(false, 0.001, 18).orderBy("CRSDepTime").show(6)

    val duration = (System.currentTimeMillis() - startTime) / 1000
    println(s"Training completed in ${duration}s, accuracy = $accuracy")

    // MLflow tracking via REST API
    val mlflowUri = sys.env.getOrElse("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    println(s"Logging to MLflow at $mlflowUri")
    try {
      import java.net.{HttpURLConnection, URL}
      import java.io.{BufferedReader, InputStreamReader}
      import scala.collection.mutable.StringBuilder

      // Create run
      val expUrl = new URL(s"$mlflowUri/api/2.0/mlflow/runs/create")
      val expConn = expUrl.openConnection().asInstanceOf[HttpURLConnection]
      expConn.setRequestMethod("POST")
      expConn.setDoOutput(true)
      expConn.setRequestProperty("Content-Type", "application/json")
      val expBody = s"""{"experiment_id":"0","run_name":"$runName","tags":[{"key":"mlflow.source.type","value":"SCALA"},{"key":"mlflow.source.name","value":"TrainModel"}]}"""
      expConn.getOutputStream.write(expBody.getBytes("UTF-8"))
      val expReader = new BufferedReader(new InputStreamReader(expConn.getInputStream))
      val respBuilder = new StringBuilder
      var line = expReader.readLine()
      while (line != null) { respBuilder.append(line); respBuilder.append('\n'); line = expReader.readLine() }
      expReader.close()
      val expResp = respBuilder.toString
      val runId = expResp.split("\"run_id\": \"", 2).lift(1).flatMap(_.split("\"", 2).headOption).getOrElse("")

      // Log params + metrics + finish in one batch
      val batchUrl = new URL(s"$mlflowUri/api/2.0/mlflow/runs/log-batch")
      val batchConn = batchUrl.openConnection().asInstanceOf[HttpURLConnection]
      batchConn.setRequestMethod("POST")
      batchConn.setDoOutput(true)
      batchConn.setRequestProperty("Content-Type", "application/json")
      val batchBody = s"""{
        "run_id":"$runId",
        "params":[
          {"key":"maxBins","value":"$maxBins"},
          {"key":"maxMemoryInMB","value":"$maxMemoryMB"},
          {"key":"numTrees","value":"$numTrees"},
          {"key":"maxDepth","value":"$maxDepth"},
          {"key":"model_version","value":"5.0"},
          {"key":"training_duration_seconds","value":"$duration"}
        ],
        "metrics":[
          {"key":"accuracy","value":$accuracy,"timestamp":$startTime,"step":0}
        ]
      }"""
      batchConn.getOutputStream.write(batchBody.getBytes("UTF-8"))
      val batchCode = batchConn.getResponseCode
      if (batchCode == 200) {
        batchConn.getInputStream.close()
      } else {
        val errReader = new BufferedReader(new InputStreamReader(batchConn.getErrorStream))
        val errBuilder = new StringBuilder
        var el = errReader.readLine()
        while (el != null) { errBuilder.append(el); el = errReader.readLine() }
        errReader.close()
        println(s"MLflow log-batch failed ($batchCode): ${errBuilder.toString}")
      }

      // Finish run
      val finishUrl = new URL(s"$mlflowUri/api/2.0/mlflow/runs/update")
      val finishConn = finishUrl.openConnection().asInstanceOf[HttpURLConnection]
      finishConn.setRequestMethod("POST")
      finishConn.setDoOutput(true)
      finishConn.setRequestProperty("Content-Type", "application/json")
      finishConn.getOutputStream.write(s"""{"run_id":"$runId","status":"FINISHED"}""".getBytes("UTF-8"))
      val finishCode = finishConn.getResponseCode
      if (finishCode == 200) {
        finishConn.getInputStream.close()
      } else {
        val errReader = new BufferedReader(new InputStreamReader(finishConn.getErrorStream))
        val errBuilder = new StringBuilder
        var el = errReader.readLine()
        while (el != null) { errBuilder.append(el); el = errReader.readLine() }
        errReader.close()
        println(s"MLflow finish failed ($finishCode): ${errBuilder.toString}")
      }

      println(s"MLflow run $runId finished, accuracy = $accuracy, duration = ${duration}s")
    } catch {
      case e: Exception => println(s"MLflow logging error (non-fatal): ${e.getMessage}")
    }

    spark.stop()
  }
}
