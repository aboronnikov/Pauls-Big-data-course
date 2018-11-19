name := "Streaming"
version := "0.1"
scalaVersion := "2.12.7"
mainClass in Compile := Some("Producer")
assemblyJarName in assembly := "streaming.jar"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.kafka" %% "kafka" % "2.0.0",
  "log4j" % "log4j" % "1.2.17",
  "com.jsuereth" %% "scala-arm" % "2.0"
)

coverageEnabled := true
coverageExcludedPackages := ".*inputprocessor"

scapegoatVersion in ThisBuild := "1.3.2"
scalaBinaryVersion in ThisBuild := "2.12"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.12" % Test,
  "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
  "org.mockito" % "mockito-all" % "1.9.5" % Test
)
