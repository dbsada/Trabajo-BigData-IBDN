package es.upm.dit.ging.predictor
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{Bucketizer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.functions.{col, concat, from_json, lit, to_json, struct}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MakePrediction {

  def main(args: Array[String]): Unit = {
    println("Flight predictor starting...")

    val spark = SparkSession
      .builder
      .appName("FlightDelayPrediction")
      .getOrCreate()
    import spark.implicits._

    // Load models from S3
    val base_path = "s3a://lakehouse"
    val arrivalBucketizerPath = "%s/models/arrival_bucketizer_2.0.bin".format(base_path)
    print(arrivalBucketizerPath.toString())
    val arrivalBucketizer = Bucketizer.load(arrivalBucketizerPath)
    val columns= Seq("Carrier","Origin","Dest","Route")

    //Load all the string field vectorizer pipelines into a dict
    val stringIndexerModel = columns.map { n => 
      val path = s"$base_path/models/string_indexer_model_$n.bin"
      StringIndexerModel.load(path)
    }

    // Load the numeric vector assembler
    val vectorAssemblerPath = "%s/models/numeric_vector_assembler.bin".format(base_path)
    val vectorAssembler = VectorAssembler.load(vectorAssemblerPath)

    // Load the classifier model
    val randomForestModelPath = "%s/models/spark_random_forest_classifier.flight_delays.5.0.bin".format(
      base_path)
    val rfc = RandomForestClassificationModel.load(randomForestModelPath)

    //Process Prediction Requests in Streaming
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "flight-delay-ml-request")
      .load()
    df.printSchema()

    // Status stream: signals when Spark starts processing a message
    val statusStream = df
      .selectExpr("'status' as key", "'PROCESSING' as value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("topic", "flight-delay-ml-status")
      .option("checkpointLocation", "/tmp/spark_checkpoint_status")
      .outputMode("append")
      .start()

    val flightJsonDf = df.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("Origin", DataTypes.StringType)
      .add("FlightNum", DataTypes.StringType)
      .add("DayOfWeek", DataTypes.IntegerType)
      .add("DayOfYear", DataTypes.IntegerType)
      .add("DayOfMonth", DataTypes.IntegerType)
      .add("Dest", DataTypes.StringType)
      .add("DepDelay", DataTypes.DoubleType)
      .add("Prediction", DataTypes.StringType)
      .add("Timestamp", DataTypes.TimestampType)
      .add("FlightDate", DataTypes.DateType)
      .add("Carrier", DataTypes.StringType)
      .add("UUID", DataTypes.StringType)
      .add("Distance", DataTypes.DoubleType)
      .add("Carrier_index", DataTypes.DoubleType)
      .add("Origin_index", DataTypes.DoubleType)
      .add("Dest_index", DataTypes.DoubleType)
      .add("Route_index", DataTypes.DoubleType)

    val flightNestedDf = flightJsonDf.select(from_json(col("value"), struct).as("flight"))

    flightNestedDf.printSchema()

    val flightFlattenedDf = flightNestedDf.selectExpr("flight.Origin",
      "flight.DayOfWeek","flight.DayOfYear","flight.DayOfMonth","flight.Dest",
      "flight.DepDelay","flight.Timestamp","flight.FlightDate",
      "flight.Carrier","flight.UUID","flight.Distance")
    flightFlattenedDf.printSchema()

    val predictionRequestsWithRouteMod = flightFlattenedDf.withColumn(
      "Route",
                concat(
                  flightFlattenedDf("Origin"),
                  lit('-'),
                  flightFlattenedDf("Dest")
                )
    )

    val flightFlattenedDf2 = flightNestedDf.selectExpr("flight.Origin",
      "flight.DayOfWeek","flight.DayOfYear","flight.DayOfMonth","flight.Dest",
      "flight.DepDelay","flight.Timestamp","flight.FlightDate",
      "flight.Carrier","flight.UUID","flight.Distance",
      "flight.Carrier_index","flight.Origin_index","flight.Dest_index","flight.Route_index")
    flightFlattenedDf2.printSchema()

    val predictionRequestsWithRouteMod2 = flightFlattenedDf2.withColumn(
      "Route",
      concat(
        flightFlattenedDf2("Origin"),
        lit('-'),
        flightFlattenedDf2("Dest")
      )
    )

    val predictionRequestsWithRoute = stringIndexerModel.map(n=>n.transform(predictionRequestsWithRouteMod))

    val vectorizedFeatures = vectorAssembler.setHandleInvalid("keep").transform(predictionRequestsWithRouteMod2)

    vectorizedFeatures.printSchema()

    val finalVectorizedFeatures = vectorizedFeatures
        .drop("Carrier_index")
        .drop("Origin_index")
        .drop("Dest_index")
        .drop("Route_index")

    finalVectorizedFeatures.printSchema()

    val predictions = rfc.transform(finalVectorizedFeatures)
      .drop("Features_vec")

    val finalPredictions = predictions.drop("indices").drop("values").drop("rawPrediction").drop("probability")

    finalPredictions.printSchema()

    // Write predictions to Kafka response topic
    val kafkaSink = finalPredictions
      .selectExpr("CAST(UUID AS STRING) as key", "to_json(struct(*)) as value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("topic", "flight-delay-ml-response")
      .option("checkpointLocation", "/tmp/spark_checkpoint_kafka")
      .outputMode("append")
      .start()

    // Console output for debug
    val consoleOutput = finalPredictions.writeStream
      .outputMode("append")
      .format("console")
      .start()
    consoleOutput.awaitTermination()
  }

}
