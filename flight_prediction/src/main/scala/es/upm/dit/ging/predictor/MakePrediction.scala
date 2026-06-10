package es.upm.dit.ging.predictor
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{Bucketizer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.functions.{col, concat, from_json, lit, to_json, struct}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

object MakePrediction {

  def main(args: Array[String]): Unit = {
    println("=" * 60)
    println("Flight predictor starting...")
    println("=" * 60)

    val spark = SparkSession
      .builder
      .appName("FlightDelayPrediction")
      .getOrCreate()
    import spark.implicits._

    val base_path = "s3a://lakehouse"
    val envVersion = sys.env.getOrElse("MODEL_VERSION", "1.0")
    val bucketizerVersion = sys.env.getOrElse("BUCKETIZER_VERSION", "1.0")

    // Read active run ID from MinIO, fall back to env var
    val modelVersion = try {
      import org.apache.hadoop.fs.{FileSystem, Path => HPath}
      val fs = FileSystem.get(new java.net.URI("s3a://lakehouse"), spark.sparkContext.hadoopConfiguration)
      val p = new HPath("s3a://lakehouse/models/active_run_id.txt")
      if (fs.exists(p)) {
        val is = fs.open(p)
        val id = scala.io.Source.fromInputStream(is).mkString.trim
        is.close()
        println(s"[SPARK] Active model from MinIO: $id")
        id
      } else {
        println(s"[SPARK] No active_run_id.txt, using env: $envVersion")
        envVersion
      }
    } catch {
      case e: Exception =>
        println(s"[SPARK] Could not read active_run_id.txt, using env: $envVersion (${e.getMessage})")
        envVersion
    }

    val arrivalBucketizerPath = s"$base_path/models/arrival_bucketizer_$bucketizerVersion.bin"
    println(s"[SPARK] Loading arrival bucketizer from: $arrivalBucketizerPath")
    val arrivalBucketizer = Bucketizer.load(arrivalBucketizerPath)

    val columns = Seq("Carrier", "Origin", "Dest", "Route")
    val stringIndexerModel = columns.map { n =>
      val path = s"$base_path/models/string_indexer_model_$n.bin"
      println(s"[SPARK] Loading string indexer: $path")
      StringIndexerModel.load(path).setHandleInvalid("keep")
    }

    val vectorAssemblerPath = s"$base_path/models/numeric_vector_assembler.bin"
    println(s"[SPARK] Loading vector assembler from: $vectorAssemblerPath")
    val vectorAssembler = VectorAssembler.load(vectorAssemblerPath)

    val randomForestModelPath = s"$base_path/models/spark_random_forest_classifier.flight_delays.$modelVersion.bin"
    println(s"[SPARK] Loading RF model from: $randomForestModelPath")
    val rfc = RandomForestClassificationModel.load(randomForestModelPath)

    println("[SPARK] All models loaded. Starting Kafka streaming...")
    val checkpointBase = s"/opt/spark/checkpoint/spark_checkpoint_$modelVersion"

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "flight-delay-ml-request")
      .option("startingOffsets", "earliest")
      .load()
    df.printSchema()

    val statusStream = df
      .selectExpr("'status' as key", "'PROCESSING' as value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("topic", "flight-delay-ml-status")
      .option("checkpointLocation", s"${checkpointBase}_status")
      .outputMode("append")
      .start()

    val flightJsonDf = df.selectExpr("CAST(value AS STRING)")

    val flightStruct = new StructType()
      .add("Origin", DataTypes.StringType)
      .add("FlightNum", DataTypes.StringType)
      .add("DayOfWeek", DataTypes.IntegerType)
      .add("DayOfYear", DataTypes.IntegerType)
      .add("DayOfMonth", DataTypes.IntegerType)
      .add("Dest", DataTypes.StringType)
      .add("DepDelay", DataTypes.DoubleType)
      .add("Timestamp", DataTypes.TimestampType)
      .add("FlightDate", DataTypes.DateType)
      .add("Carrier", DataTypes.StringType)
      .add("UUID", DataTypes.StringType)
      .add("Distance", DataTypes.DoubleType)

    val flightNestedDf = flightJsonDf.select(from_json(col("value"), flightStruct).as("flight"))

    val flightParsed = flightNestedDf.selectExpr(
      "flight.Origin", "flight.FlightNum",
      "flight.DayOfWeek", "flight.DayOfYear", "flight.DayOfMonth",
      "flight.Dest", "flight.DepDelay", "flight.Timestamp", "flight.FlightDate",
      "flight.Carrier", "flight.UUID", "flight.Distance"
    )

    val withRoute = flightParsed.withColumn(
      "Route", concat(col("Origin"), lit('-'), col("Dest"))
    )

    var indexedDf = withRoute
    for (model <- stringIndexerModel) {
      indexedDf = model.transform(indexedDf)
    }

    val vectorizedFeatures = vectorAssembler.setHandleInvalid("keep").transform(indexedDf)

    val predictions = rfc.transform(vectorizedFeatures)

    val loggingSink = predictions.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val rows = batchDF.collect()
        if (rows.nonEmpty) {
          println(s"[SPARK] === Processing batch $batchId with ${rows.length} prediction(s) ===")
          rows.foreach { row =>
            val uuid = row.getAs[String]("UUID")
            val prediction = row.getAs[Double]("Prediction").toInt
            val carrier = row.getAs[String]("Carrier")
            val origin = row.getAs[String]("Origin")
            val dest = row.getAs[String]("Dest")
            val depDelay = row.getAs[Double]("DepDelay")
            val delayLabel = if (prediction == 0) "ON_TIME" else "DELAYED"
            println(s"[SPARK] PREDICTION: UUID=${uuid.take(12)}... | $carrier $origin->$dest | DepDelay=$depDelay | Result=$delayLabel (class=$prediction)")
          }
          import org.apache.spark.sql.functions.{col, struct, to_json}
          // Write to Kafka response topic directly from foreachBatch
          val requiredCols = Seq("UUID", "Origin", "Dest", "Carrier", "Route", "FlightNum")
          val validDF = requiredCols.foldLeft(batchDF) { (df, c) => df.filter(col(c).isNotNull) }
          val responseDF = validDF.select(
            col("UUID").cast("string").as("key"),
            to_json(struct(
              col("UUID"), col("Origin"), col("Dest"), col("Carrier"),
              col("FlightDate"), col("FlightNum"), col("DepDelay"),
              col("Distance"), col("Route"),
              col("DayOfYear"), col("DayOfMonth"), col("DayOfWeek"), col("Prediction")
            )).as("value")
          )
          responseDF.write
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("topic", "flight-delay-ml-response")
            .save()
          println(s"[SPARK] Written ${rows.length} response(s) to Kafka")
        }
      }
      .option("checkpointLocation", s"${checkpointBase}_prediction")
      .outputMode("update")
      .start()

    println("[SPARK] Streaming queries started. Waiting for predictions...")
    loggingSink.awaitTermination()
  }

}
