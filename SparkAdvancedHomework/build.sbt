name := "SparkAdvancedHomework"

version := "0.1"

scalaVersion := "2.11.7"
mainClass in Compile := Some("program.TopicDataIngester")
assemblyJarName in assembly := "TopicDataIngester.jar"

val sparkVersion = "2.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.kafka" %% "kafka" % "2.0.0",
  "log4j" % "log4j" % "1.2.17",
  "org.scala-lang" % "scala-library" % "2.11.7",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  "com.jayway.jsonpath" % "json-path" % "0.9.1",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.9.6",
  "commons-cli" % "commons-cli" % "1.2",
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.8.0"
)

scalacOptions ++= Seq("-deprecation", "-feature")

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.6"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.6"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.6"

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.12" % Test,
  "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test
)

scapegoatVersion in ThisBuild := "1.3.2"
scalaBinaryVersion in ThisBuild := "2.11"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}