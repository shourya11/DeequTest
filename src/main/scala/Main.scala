import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.VerificationSuite
import org.apache.spark.sql.types.{ArrayType, DoubleType, MapType, StringType, StructType}
import com.amazon.deequ.analyzers.{Analysis, Analyzer, Completeness, Compliance, Distinctness, InMemoryStateProvider, MaxLength, Size}
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.metrics.Metric
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions.{col, current_timestamp, explode, lit}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType

object Main {

  val spark : SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  val Schema = new StructType().add("Deequ", new StructType().add("Analysers", ArrayType(new StructType()
    .add("function",StringType,nullable = true)
    .add("columnName",StringType,nullable = true)
    .add("condition",StringType,nullable = true)
  )))
    .add("Source",new StructType()
      .add("Path",StringType)
      .add("Format",StringType))
//    .add("Destination",new StructType()
//      .add("Path",StringType))


  var data = spark.read.option("multiLine","true").schema(Schema).format("json").load("C:\\Users\\shour\\Desktop\\Whiteklay\\inputJson.json")

  data.show()
  var Path = data.select($"Source.*").select($"Path").head().toString
  Path = Path.substring(1, Path.length()-1)

//  var DestPath = data.select($"Destination.*").select($"Path").head().toString
//  DestPath = DestPath.substring(1, DestPath.length()-1)

  var Format = data.select($"Source.*").select($"Format").head().toString
  Format = Format.substring(1, Format.length()-1)

  var DeequData = data.select(explode($"Deequ.Analysers").as("Analysers")).select($"Analysers.*")

  val dataCollected = DeequData.collect()

  println("transforming ingested json")

  //  var b = AnalysisRunner.onData()

  var analysers = DeequSeq.AnalyzerArr(dataCollected)
  var b = DeequSeq.AnalyzerSeq(analysers)
  println(b)

  Streaming.run(Format,Path,b)
}
