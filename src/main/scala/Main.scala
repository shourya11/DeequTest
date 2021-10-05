import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object Main {

  val spark: SparkSession = SparkSession.builder().getOrCreate()

  import spark.implicits._

  def main(jsonPath: String): Unit = {

    val data = spark.read.option("multiLine", "true").schema(SchemaData.inputJsonSchema).format("json").load(jsonPath)
    data.show()

    var InputPath = data.select($"Source.*").select($"Path").head().toString
    InputPath = InputPath.substring(1, InputPath.length() - 1)

    var InputFormat = data.select($"Source.*").select($"Format").head().toString
    InputFormat = InputFormat.substring(1, InputFormat.length() - 1)

    var DestPath = data.select($"Destination.*").select($"Path").head().toString
    DestPath = DestPath.substring(1, DestPath.length() - 1)

    var DestFormat = data.select($"Destination.*").select($"Format").head().toString
    DestFormat = DestFormat.substring(1, DestFormat.length() - 1)

    var DestMode = data.select($"Destination.*").select($"Mode").head().toString
    DestMode = DestMode.substring(1, DestMode.length() - 1)

    val DeequAnalyzers = data.select(explode($"Deequ.Analysers").as("Analysers")).select($"Analysers.*")

    val DeequChecks = data.select(explode($"Deequ.Checks").as("Checks")).select($"Checks.*")

    val AnalyzersCollected = DeequAnalyzers.collect()
    val ChecksCollected = DeequChecks.collect()

    println("transforming ingested json")

    //  var b = AnalysisRunner.onData()

    val analysers = Analyzers.AnalyzerArr(AnalyzersCollected)

    val checks = Checks.ChecksSeq(ChecksCollected)

//    println(checks)

    Streaming.run(InputFormat, InputPath,  DestFormat, DestPath, DestMode, analysers, checks)

  }
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
