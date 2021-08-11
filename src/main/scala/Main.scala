import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions.explode

object Main {

  val spark : SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  var data = spark.read.option("multiLine","true").schema(SchemaData.inputJsonSchema).format("json").load("C:\\Users\\shour\\Desktop\\Whiteklay\\inputJson.json")

  data.show()
  var Path = data.select($"Source.*").select($"Path").head().toString
  Path = Path.substring(1, Path.length()-1)

//  var DestPath = data.select($"Destination.*").select($"Path").head().toString
//  DestPath = DestPath.substring(1, DestPath.length()-1)
  //add destination when needed in the json

  var Format = data.select($"Source.*").select($"Format").head().toString
  Format = Format.substring(1, Format.length()-1)

  var DeequAnalyzers = data.select(explode($"Deequ.Analysers").as("Analysers")).select($"Analysers.*")

  var DeequChecks = data.select(explode($"Deequ.Checks").as("Checks")).select($"Checks.*")

  val AnalyzersCollected = DeequAnalyzers.collect()
  val ChecksCollected = DeequChecks.collect()

  println("transforming ingested json")

  //  var b = AnalysisRunner.onData()

  var analysers = Analyzers.AnalyzerArr(AnalyzersCollected)

  var checks = Checks.ChecksSeq(ChecksCollected)
  println(checks)


  Streaming.run(Format,Path,analysers,checks)
}


//def analyser(renamedData: DataFrame): DataFrame = {
//  val analysis: AnalyzerContext = {
//    AnalysisRunner.onData(renamedData)
//      .addAnalyzer(Size())
//      .addAnalyzer(Distinctness("object_class"))
//      .addAnalyzer(Completeness("object_class"))
//      .addAnalyzer(Completeness("agreement_number"))
//      .addAnalyzer(Completeness("sysAudit_object_class"))
//      .run()
//  }
//  successMetricsAsDataFrame(spark, analysis)
//
//}
