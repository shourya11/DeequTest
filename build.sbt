name := "DeequTest"

version := "0.1"

scalaVersion := "2.12.11"

val scala_tools_version = "2.12"
val spark_version = "3.1.1"
libraryDependencies += "org.apache.spark" % ("spark-core_" + scala_tools_version) % spark_version % "provided"
libraryDependencies += "org.apache.spark" % ("spark-sql_" + scala_tools_version) % spark_version % "provided"
libraryDependencies += "com.amazon.deequ" % "deequ" % "2.0.0-spark-3.1"

// https://mvnrepository.com/artifact/com.amazon.deequ/deequ
//libraryDependencies += "com.amazon.deequ" % "deequ" % "1.1.0_spark-2.4-scala-2.11"
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
//libraryDependencies += "com.typesafe.play" %% "play-json" % "2.5.12"
//libraryDependencies += "org.scala-lang.modules" %% "scala-async" % "0.10.0"
//libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
