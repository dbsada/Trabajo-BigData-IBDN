import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.functions.{col, concat, from_json, lit, struct, to_json}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

import scala.collection.mutable

object MakePrediction {
  def main(args: Array[String]): Unit = {
    println("Flight predictor starting...")

    val spark = SparkSession.builder
      .appName("FlightDelayPrediction")
      .getOrCreate()

    import spark.implicits._

    val basePath = "s3a://lakehouse"

    def waitForStringIndexers(): Seq[StringIndexerModel] = {
      val names = Seq("Carrier", "Origin", "Dest", "Route")
      while (true) {
        val loaded = names.flatMap { colName =>
          val path = s"$basePath/models/string_indexer_model_$colName.bin"
          try {
            println(s"[SPARK] Loading string indexer: $path")
            Some(StringIndexerModel.load(path).setHandleInvalid("keep"))
          } catch {
            case _: Exception =>
              println(s"[SPARK] StringIndexer $colName not ready, retrying in 10s...")
              None
          }
        }
        if (loaded.length == names.length) {
          println("[SPARK] All StringIndexers loaded")
          return loaded
        }
        Thread.sleep(10000)
      }
      Seq.empty
    }

    def waitForVectorAssembler(): VectorAssembler = {
      val path = s"$basePath/models/numeric_vector_assembler.bin"
      while (true) {
        try {
          val va = VectorAssembler.load(path)
          println("[SPARK] VectorAssembler loaded")
          return va
        } catch {
          case _: Exception =>
            println(s"[SPARK] VectorAssembler not ready, retrying in 10s...")
            Thread.sleep(10000)
        }
      }
      null
    }

    val stringIndexerModels = waitForStringIndexers()
    val vectorAssembler = waitForVectorAssembler()
    val modelosCache = mutable.Map[String, RandomForestClassificationModel]()

    val inputSchema = new StructType()
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
      .add("model_ids", DataTypes.StringType)

    val kafkaStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", sys.env.getOrElse("KAFKA", "kafka:9092"))
      .option("subscribe", sys.env.getOrElse("TOPIC_IN", "request"))
      .option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", "100")
      .load()

    val mensajesParseados = kafkaStream
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), inputSchema).as("datos"))
      .selectExpr(
        "datos.Origin", "datos.FlightNum",
        "datos.DayOfWeek", "datos.DayOfYear", "datos.DayOfMonth",
        "datos.Dest", "datos.DepDelay", "datos.Timestamp", "datos.FlightDate",
        "datos.Carrier", "datos.UUID", "datos.Distance",
        "datos.model_ids"
      )

    val datosConRoute = mensajesParseados.withColumn(
      "Route", concat(col("Origin"), lit("-"), col("Dest"))
    )

    var datosIndexados = datosConRoute
    for (model <- stringIndexerModels) {
      datosIndexados = model.transform(datosIndexados)
    }

    val vectorizado = vectorAssembler.setHandleInvalid("keep").transform(datosIndexados)

    val procesoStreaming = vectorizado.writeStream
      .trigger(Trigger.ProcessingTime(2, java.util.concurrent.TimeUnit.SECONDS))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        val modelosSolicitados = batchDF
          .select("model_ids")
          .where(col("model_ids").isNotNull)
          .distinct()
          .as[String]
          .collect()
          .flatMap { ids =>
            ids.stripPrefix("[").stripSuffix("]").split(",")
              .map(_.trim.replace("\"", ""))
              .filter(_.nonEmpty)
          }
          .distinct

        for (mid <- modelosSolicitados) {
          if (!modelosCache.contains(mid)) {
            val path = s"$basePath/models/spark_random_forest_classifier.flight_delays.$mid.bin"
            var loaded = false
            var attempts = 0
            while (!loaded && attempts < 10) {
              try {
                modelosCache(mid) = RandomForestClassificationModel.load(path)
                println(s"[SPARK] Model $mid loaded and cached")
                loaded = true
              } catch {
                case e: Exception =>
                  attempts += 1
                  println(s"[SPARK] Loading model $mid (attempt $attempts/10): ${e.getMessage}")
                  if (attempts < 10) Thread.sleep(5000)
              }
            }
          }
        }

        if (!batchDF.isEmpty) {
          var resultadoFinal: DataFrame = null
          for (mid <- modelosSolicitados) {
            val modelo = modelosCache.getOrElse(mid, null)
            if (modelo != null) {
              val predicciones = modelo.transform(batchDF)
              val soloValidos = Seq("UUID", "Origin", "Dest", "Carrier", "Route", "FlightNum")
                .foldLeft(predicciones) { (df, c) => df.filter(col(c).isNotNull) }

              if (!soloValidos.isEmpty) {
                val conModelId = soloValidos.select(
                  to_json(struct(
                    col("UUID"), lit(mid).as("model_id"),
                    col("Origin"), col("Dest"), col("Carrier"),
                    col("FlightDate"), col("FlightNum"), col("DepDelay"),
                    col("Distance"), col("Route"),
                    col("DayOfYear"), col("DayOfMonth"), col("DayOfWeek"),
                    col("Prediction"), col("Timestamp")
                  )).as("value")
                )
                resultadoFinal = if (resultadoFinal == null) conModelId
                                else resultadoFinal.union(conModelId)
              }
            }
          }

          if (resultadoFinal != null && !resultadoFinal.isEmpty) {
            resultadoFinal.write
              .format("kafka")
              .option("kafka.bootstrap.servers", sys.env.getOrElse("KAFKA", "kafka:9092"))
              .option("topic", sys.env.getOrElse("TOPIC_OUT", "response"))
              .save()
          }
        }
      }
      .option("checkpointLocation", "/opt/spark/checkpoint/spark_checkpoint_prediction")
      .outputMode("update")
      .start()

    println("[SPARK] Streaming queries started. Waiting for predictions...")
    procesoStreaming.awaitTermination()
  }
}