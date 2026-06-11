import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{Bucketizer, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{concat, lit}

import java.util.UUID

object TrainModel {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()

    // Obtener hiperparámetros -------------------------------------------------
    var numTrees = 20
    var maxDepth = 10
    var runName = "RandomForest"

    args.grouped(2).foreach {
        case Array("--num-trees", v) => numTrees = v.toInt
        case Array("--max-depth", v) => maxDepth = v.toInt
        case Array("--run-name", v) => runName = v
        case other => println(s"Warning: ignoring unknown argument: ${other.mkString(" ")}")
    }

    println(s"Training with: numTrees=$numTrees maxDepth=$maxDepth runName=$runName")
    // --------------------------------------------------------------------------

    
    // Crear la sesión de Spark (conexión al cluster)----------------------------
    val spark = SparkSession.builder
      .appName("TrainRandomForest")
      .getOrCreate()

    import spark.implicits._
    // --------------------------------------------------------------------------


    // Definir el esquema de los datos (tipos de cada columna)-------------------
    val schema = StructType(Seq(
      // ArrDelay = lo que queremos predecir (minutos de retraso real)
      StructField("ArrDelay", DoubleType, nullable = true),

      // CRSDepTime y CRSArrTime = hora programada de salida/llegada
      StructField("CRSDepTime", TimestampType, nullable = true),
      StructField("CRSArrTime", TimestampType, nullable = true),

      // Carrier = código de aerolínea (AA, DL, UA...)
      StructField("Carrier", StringType, nullable = true),

      // Día del mes, semana y año (features numéricas)
      StructField("DayOfMonth", IntegerType, nullable = true),
      StructField("DayOfWeek", IntegerType, nullable = true),
      StructField("DayOfYear", IntegerType, nullable = true),

      // DepDelay = retraso inicial en minutos
      StructField("DepDelay", DoubleType, nullable = true),

      // Dest y Origin = aeropuertos de destino y origen
      StructField("Dest", StringType, nullable = true),
      StructField("Origin", StringType, nullable = true),

      // Distance = distancia del vuelo en millas
      StructField("Distance", DoubleType, nullable = true),

      // FlightNum y FlightDate NO se usan como features,
      // solo están en el schema para poder leer los datos
      StructField("FlightNum", StringType, nullable = true),
      StructField("FlightDate", DateType, nullable = true),
    ))
    // --------------------------------------------------------------------------

    
    // Lectura de los datos desde MinIO -----------------------------------------
    val defaultVersion = sys.env.getOrElse("MODEL_VERSION", "1.0")
    val bucketizerVersion = sys.env.getOrElse("BUCKETIZER_VERSION", "1.0")

    println(s"Using default version: $defaultVersion, bucketizer: $bucketizerVersion")

    val inputPath = "s3a://lakehouse/raw/simple_flight_delay_features.jsonl.bz2"

    var features = spark.read.schema(schema).json(inputPath).repartition(4)
    features.first()

    features.writeTo("lakehouse.flight_delays").createOrReplace()
    println("Table created/updated")

    features = spark.table("lakehouse.flight_delays")
    // --------------------------------------------------------------------------

    
    // Nueva feature: Route = "Origin-Dest" (ej: "JFK-LAX") ---------------------
    val featuresWithRoute = features.withColumn(
      "Route",
      concat($"Origin", lit("-"), $"Dest")
    )
    featuresWithRoute.show(6)
    // --------------------------------------------------------------------------


    // Bucketizar (categorizar retraso) -----------------------------------------
    //   ArrDelay   →   Bucket   →   Significado
    //   (-∞, -15)       0           Muy pronto (ON_TIME)
    //   [-15, 0)        1           Ligeramente pronto
    //   [0, 30)         2           Ligeramente tarde (DELAYED)
    //   [30, +∞)        3           Muy tarde (DELAYED)

    val splits = Array(
      Double.NegativeInfinity,  // desde -∞
      -15.0,                    // hasta -15
      0.0,                      // hasta 0
      30.0,                     // hasta 30
      Double.PositiveInfinity   // hasta +∞
    )

    val bucketizer = new Bucketizer()
      .setSplits(splits)
      .setInputCol("ArrDelay")         // columna original
      .setOutputCol("ArrDelayBucket")  // columna con la categoría (0, 1, 2, 3)

    val bucketizerPath = s"s3a://lakehouse/models/arrival_bucketizer_$bucketizerVersion.bin"
    bucketizer.write.overwrite.save(bucketizerPath)
    println(s"Bucketizer saved to $bucketizerPath")

    var mlFeatures = bucketizer.transform(featuresWithRoute)
    mlFeatures.select("ArrDelay", "ArrDelayBucket").show()
    // --------------------------------------------------------------------------


    // String indexer (convertir texto a números) -------------------------------
    val stringColumns = Seq("Carrier", "Origin", "Dest", "Route") // columnas que convertir

    for (col <- stringColumns) {
      val indexer = new StringIndexer()
        .setInputCol(col)
        .setOutputCol(col + "_index")
        .setHandleInvalid("keep")

      val model = indexer.fit(mlFeatures)
      mlFeatures = model.transform(mlFeatures).drop(col)

      val path = s"s3a://lakehouse/models/string_indexer_model_$col.bin"
      model.write.overwrite.save(path)
      println(s"StringIndexer for $col saved")
    }
    // --------------------------------------------------------------------------


    // VectorAssembler (combinar features en un vector) -------------------------
    val numericCols = Array("DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear")
    val indexCols = Array("Carrier_index", "Origin_index", "Dest_index", "Route_index")

    val assembler = new VectorAssembler()
      .setInputCols(numericCols ++ indexCols)  // 9 columnas de entrada
      .setOutputCol("Features_vec")            // 1 vector de salida

    val finalVectorized = assembler.transform(mlFeatures)

    val assemblerPath = "s3a://lakehouse/models/numeric_vector_assembler.bin"
    assembler.write.overwrite.save(assemblerPath)
    println(s"VectorAssembler saved to $assemblerPath")
    // --------------------------------------------------------------------------


    // Entrenar el modelo de Random Forest --------------------------------------
    val rfc = new RandomForestClassifier()
      .setFeaturesCol("Features_vec")       // vector con las 9 features
      .setLabelCol("ArrDelayBucket")         // lo que queremos predecir (0, 1, 2, 3)
      .setPredictionCol("Prediction")        // columna donde guarda el resultado
      .setMaxBins(4657)                      // para capturar todos los aeropuertos
      .setMaxMemoryInMB(1024)                // memoria máxima para el proceso
      .setNumTrees(numTrees)                 // número de árboles (argumento)
      .setMaxDepth(maxDepth)                 // profundidad máxima (argumento)

    val model = rfc.fit(finalVectorized)
    println("Model trained successfully")

    // Evaluar precisión sobre los mismos datos de entrenamiento -----------------
    val predictions = model.transform(finalVectorized)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("ArrDelayBucket")
      .setPredictionCol("Prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Training accuracy: $accuracy")
    // --------------------------------------------------------------------------

    val modelVersion = UUID.randomUUID().toString.replace("-", "")
    val modelPath = s"s3a://lakehouse/models/spark_random_forest_classifier.flight_delays.$modelVersion.bin"
    model.write.overwrite.save(modelPath)
    println(s"Model saved to $modelPath")

    // Guardar precisión en MinIO para que Python pueda leerla ------------------
    val duration = (System.currentTimeMillis() - startTime) / 1000
    val jsonStr = s"""{"accuracy":$accuracy,"model_version":"$modelVersion","num_trees":$numTrees,"max_depth":$maxDepth,"training_duration_seconds":$duration,"run_name":"$runName"}"""
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = new java.net.URI("s3a://lakehouse")
    val outStream = org.apache.hadoop.fs.FileSystem.get(fs, conf)
      .create(new org.apache.hadoop.fs.Path(s"s3a://lakehouse/models/accuracy.$modelVersion.json"))
    outStream.write(jsonStr.getBytes("UTF-8"))
    outStream.close()
    println(s"Accuracy saved to s3a://lakehouse/models/accuracy.$modelVersion.json")

    // Guardar la version del ultimo modelo entrenado (para Airflow) -----------
    val verPath = new org.apache.hadoop.fs.Path("s3a://lakehouse/models/last_trained_version.txt")
    val verStream = org.apache.hadoop.fs.FileSystem.get(fs, conf)
      .create(verPath, true)
    verStream.write(modelVersion.getBytes("UTF-8"))
    verStream.close()
    println(s"Last trained version saved: $modelVersion")
    // --------------------------------------------------------------------------
    // --------------------------------------------------------------------------

    println(s"Training completed in ${duration}s")
    println(s"RUN_NAME=$runName") 
    println(s"MODEL_VERSION=$modelVersion") 

    spark.stop()
  }
}