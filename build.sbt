import sbtassembly.MergeStrategy
import sbt.io.Using
import sun.security.tools.PathList

//name := "bigdata-mx-2"
//
//version := "0.1"
//
//scalaVersion := "2.12.7"
//
//libraryDependencies ++=Seq(
//  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
//  "org.apache.parquet" % "parquet-hadoop" % "1.10.0",
//  "junit" % "junit" % "4.12" % Test,
//  "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test,
//  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
//  "org.scala-lang" % "scala-library" % "2.12.7"
//)
//
//packageOptions in (Compile, packageBin) +=  {
//  val file = new java.io.File("META-INF/MANIFEST.MF")
//  val manifest = Using.fileInputStream(file)( in => new java.util.jar.Manifest(in) )
//  Package.JarManifest( manifest )
//}




lazy val root = (project in file(".")).
  settings(
    name := "bigdata-mx-2",
    version := "0.1",
    scalaVersion := "2.12.7",
    mainClass in Compile := Some("Main")
  )

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
  "org.apache.parquet" % "parquet-hadoop" % "1.10.0",
  "junit" % "junit" % "4.12" % Test,
  "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
  "org.scala-lang" % "scala-library" % "2.12.7"
)

publishTo := Some(Resolver.sftp("Server", "ecsc00a022c6.epam.com", "22"))


//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF") => sbtassembly.MergeStrategy.discard
//  case x => sbtassembly.MergeStrategy.first
//}

// META-INF discarding
//mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
//  case PathList("META-INF", xs@_*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}
//}