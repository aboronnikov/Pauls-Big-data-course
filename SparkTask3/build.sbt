name := "SparkTask3"
version := "0.1"
scalaVersion := "2.12.7"
mainClass in Compile := Some("unknwon")
assemblyJarName in assembly := "sparktask3.jar"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

coverageEnabled := true

scapegoatVersion in ThisBuild := "1.3.2"
scalaBinaryVersion in ThisBuild := "2.12"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
