name := "flight_prediction"
version := "0.1"
scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "4.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "4.1.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "4.1.1" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "4.1.1" % "provided",
)
