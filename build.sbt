name := "bigdata-mx-2"

version := "0.1"

scalaVersion := "2.12.7"

mainClass in Compile := Some("Main")

lazy val root = Project(id = "root", base = file(".")) aggregate(hdfs)
lazy val hdfs = Project(id = "hdfs", base = file("HDFS"))

coverageEnabled := true

scapegoatVersion in ThisBuild := "1.3.2"
scalaBinaryVersion in ThisBuild := "2.12"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}