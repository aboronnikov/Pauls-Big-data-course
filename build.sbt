name := "bigdata-mx-2"

version := "0.1"
scalaVersion := "2.12.7"

lazy val root = Project(id = "root", base = file(".")) aggregate(streaming, streamingConsumer)
lazy val streaming = Project(id = "streaming", base = file("Streaming"))
lazy val streamingConsumer = Project(id = "streamingConsumer", base = file("StreamingConsumer"))

coverageEnabled := true

scapegoatVersion in ThisBuild := "1.3.2"
scalaBinaryVersion in ThisBuild := "2.12"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
