name := "StreamingProducer"
version := "0.1"
scalaVersion := "2.11.7"
mainClass in Compile := Some("com.epam.sparkproducer.program.Producer")
assemblyJarName in assembly := "streamingProducer.jar"

val sparkVersion = "2.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.kafka" %% "kafka" % "2.0.0",
  "log4j" % "log4j" % "1.2.17",
  "org.scala-lang" % "scala-library" % "2.11.7",
  "com.jsuereth" %% "scala-arm" % "2.0",
  "commons-cli" % "commons-cli" % "1.2",
  "org.apache.commons" % "commons-lang3" % "3.0",
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0"
)

coverageEnabled := false
coverageExcludedPackages := "com.epam.sparkproducer.program"

scapegoatVersion in ThisBuild := "1.3.2"
scalaBinaryVersion in ThisBuild := "2.11"

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
