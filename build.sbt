name := "DeequTest"

version := "0.1"

scalaVersion := "2.11.12"

scalaVersion := "2.11.12"
val scala_tools_version = "2.11"
val spark_version = "2.4.3"
libraryDependencies += "org.apache.spark" % ("spark-core_" + scala_tools_version) % spark_version % "provided"
libraryDependencies += "org.apache.spark" % ("spark-sql_" + scala_tools_version) % spark_version % "provided"
// https://mvnrepository.com/artifact/com.amazon.deequ/deequ
libraryDependencies += "com.amazon.deequ" % "deequ" % "1.1.0_spark-2.4-scala-2.11"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.5.12"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
