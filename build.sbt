lazy val root = (project in file(".")).
  settings(
    name := "bigdata-mx-2",
    version := "0.1",
    scalaVersion := "2.12.7",
    mainClass in Compile := Some("Main"),
    assemblyJarName in assembly := "program.jar"
  )

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
  "org.apache.parquet" % "parquet-hadoop" % "1.10.0",
  "junit" % "junit" % "4.12" % Test,
  "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
  "org.scala-lang" % "scala-library" % "2.12.7"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}