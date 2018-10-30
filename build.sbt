import sbt.io.Using

name := "bigdata-mx-2"

version := "0.1"

scalaVersion := "2.12.7"

libraryDependencies ++=Seq(
  "junit" % "junit" % "4.10" % Test,
  "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test,
  "org.apache.parquet" % "parquet-avro" % "1.10.0",
  "org.apache.hadoop" % "hadoop-common" % "3.1.1",
  "org.scala-lang" % "scala-library" % "2.12.7",
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
  "org.apache.hadoop" % "hadoop-core" % "1.2.1"
)

packageOptions in (Compile, packageBin) +=  {
  val file = new java.io.File("META-INF/MANIFEST.MF")
  val manifest = Using.fileInputStream(file)( in => new java.util.jar.Manifest(in) )
  Package.JarManifest( manifest )
}