name := "bigdata-mx-2"

version := "0.1"
scalaVersion := "2.12.7"

lazy val root = Project(id = "root", base = file(".")) aggregate(streamingProducer, streamingConsumer, hdfs)
lazy val streamingProducer = Project(id = "streamingProducer", base = file("StreamingProducer"))
lazy val streamingConsumer = Project(id = "streamingConsumer", base = file("StreamingConsumer"))
lazy val hdfs = Project(id = "hdfs", base = file("HDFS"))

coverageEnabled := true

scapegoatVersion in ThisBuild := "1.3.2"
scalaBinaryVersion in ThisBuild := "2.12"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
