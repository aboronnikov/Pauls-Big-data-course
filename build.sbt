name := "bigdata-mx-2"

version := "0.1"

scalaVersion := "2.12.7"

libraryDependencies ++=Seq(
  "junit" % "junit" % "4.10" % Test,
  "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test,
  "org.apache.parquet" % "parquet-avro" % "1.10.0",
  "org.apache.hadoop" % "hadoop-common" % "3.1.1"
)