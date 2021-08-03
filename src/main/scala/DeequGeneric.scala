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
import org.apache.spark.sql.types.StringType

object DeequGeneric {

  val spark : SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

//  var dataSeq = Seq[Analyzer[_,Metric[_]]]()
  var dataSeq = Seq[Any]()
  var arr = Array[(Any,Any,Any)]()
  var i = 0
  var j = 0


  val Schema = new StructType().add("Deequ", new StructType().add("Analysers", ArrayType(new StructType()
    .add("function",StringType,nullable = true)
    .add("columnName",StringType,nullable = true)
    .add("condition",StringType,nullable = true)
  )))

  var data = spark.read.option("multiLine","true").schema(Schema).format("json").load("C:\\Users\\shour\\Desktop\\Whiteklay\\inputJson.json")
    .select(explode($"Deequ.Analysers").as("Analysers")).select($"Analysers.*")

  val dataCollected = data.collect()

//  function columnName condition

  def AnalyzerSeq(x: Array[org.apache.spark.sql.Row]) = {
    for (i <- Range(0,x.length)){
      if (x(i).get(1) == null){
        arr = arr :+ (x(i).get(0).toString,null,null)

      }
      else if (x(i).get(2) == null ){
        arr = arr :+ (x(i).get(0).toString,x(i).get(1).toString,null)
      }
      else {
        arr = arr :+ (x(i).get(0).toString,x(i).get(1).toString,x(i).get(2).toString)
      }
    }
    arr
  }

//  val ign = "C:\\Users\\shour\\Desktop\\Whiteklay\\DeequTest\\"

  println("transforming ingested json")

  //  var b = AnalysisRunner.onData()
  var b = Analysis()
  var analysers = AnalyzerSeq(dataCollected)
  analysers.foreach{
    case ("Size",null,null) => {
      b = b.addAnalyzer(Size())
    }
      case("Completeness",str,null) => {
        b  = b.addAnalyzer(Completeness(str.toString))
      }
    case("MaxLength",str,null) => {
      b  = b.addAnalyzer(MaxLength(str.toString))
    }
    case("Compliance",str,str2) => {
      b  = b.addAnalyzer(Compliance(str.toString,str2.toString))
    }
    case _ => "invalid"
  }

  println(b)

  var base_df = spark.read.schema(SchemaData.jsonSourceSchema).format("json").load("C:\\Users\\shour\\Desktop\\Whiteklay\\data\\*.json")
  val empty_df = base_df.where("0 = 1")
  val l1: Long = 0

  spark.sql("DROP TABLE IF EXISTS trades_delta")
  spark.sql("DROP TABLE IF EXISTS bad_records")
  spark.sql("DROP TABLE IF EXISTS deequ_metrics")

  base_df.createOrReplaceTempView("trades_historical")
  empty_df.write.format("parquet").saveAsTable("trades_delta")
  empty_df.withColumn("batchID",lit(l1)).write.format("parquet").saveAsTable("bad_records")

  val stateStoreCurr = InMemoryStateProvider()
  val stateStoreNext = InMemoryStateProvider()

  println("reading data")
  val original_data = spark.readStream
    .schema(SchemaData.jsonSourceSchema)
    .option("maxFilesPerTrigger",20)
    .format("json")
    .load("C:\\Users\\shour\\Desktop\\Whiteklay\\data\\*.json")

  val renamedData = RenameData.dataRenamed(original_data)

  renamedData
    .writeStream
    //        .outputMode("update")
    //        .trigger(Trigger.Once())
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      // reassign our current state to the previous next state
      val stateStoreCurr = stateStoreNext

      // run our analysis on the current batch, aggregate with saved state
      val metricsResult = AnalysisRunner.run(
        data = batchDF,
        analysis = b,
        aggregateWith = Some(stateStoreCurr),
        saveStatesWith = Some(stateStoreNext))

      val verificationResult = VerificationSuite()
        .onData(batchDF)
        .addCheck(
          Check(CheckLevel.Error, "objectClass check")
            .isContainedIn("object_class", DataArrays.object_classArray)
        )
        .addCheck(
          Check(CheckLevel.Error, "agreementNumber check")
            .areComplete(Seq("agreement_number"))
        )
        .run()

      val x = checkResultsAsDataFrame(spark, verificationResult)

      if (verificationResult.status != CheckStatus.Success) {
        x.write.format("parquet").mode("overwrite").saveAsTable("bad_records")
        x.show()
      }
      //          val dataVerification = Deequ.verification(renamedData)


      val metric_results = successMetricsAsDataFrame(spark, metricsResult)
        .withColumn("ts", current_timestamp())

      metric_results.show()

      metric_results.write.format("parquet").mode("Overwrite").saveAsTable("deequ_metrics")

      //          Main.main()
      println("back in streaming")

    }
    .start()
    .awaitTermination()


  //  val dfs = Array((ign, "agreement")
//  )
//
//  analysers.foreach{
//    case (doj,format) => if(1 == 2){
//
//    }
//  }

  //      val analysis = Analysis()
  //        .addAnalyzers(AnalyzerSeq(dataCollected))
  ////        .addAnalyzers(rtty(""))

//  val analysis2 = Analysis()
//    .addAnalyzer(Size())
//    .addAnalyzer(Completeness("agreement_number"))
//    .addAnalyzer(MaxLength("object_class"))
//
//  //  println(analysis)
//  println(analysis2)
//
//  //  println(analysis.getClass)
//  println("\n")
//  println(analysis2.getClass)


}
