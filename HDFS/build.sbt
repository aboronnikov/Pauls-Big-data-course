name := "HDFS"
version := "0.1"
scalaVersion := "2.12.7"
mainClass in Compile := Some("com.epam.hdfs.inputprocessor.Runner")
assemblyJarName in assembly := "program.jar"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
  "org.apache.parquet" % "parquet-hadoop" % "1.10.0",
  "junit" % "junit" % "4.12" % Test,
  "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
  "org.scala-lang" % "scala-library" % "2.12.7",
  "com.sksamuel.scapegoat" %% "scalac-scapegoat-plugin" % "1.3.8"
)

coverageEnabled := true

scapegoatVersion in ThisBuild := "1.3.2"
scalaBinaryVersion in ThisBuild := "2.12"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}