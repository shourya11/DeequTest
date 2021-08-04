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

  val Schema = new StructType().add("Deequ", new StructType().add("Analysers", ArrayType(new StructType()
    .add("function",StringType,nullable = true)
    .add("columnName",StringType,nullable = true)
    .add("condition",StringType,nullable = true)
  )))

  var data = spark.read.option("multiLine","true").schema(Schema).format("json").load("C:\\Users\\shour\\Desktop\\Whiteklay\\inputJson.json")
    .select(explode($"Deequ.Analysers").as("Analysers")).select($"Analysers.*")

  val dataCollected = data.collect()

  println("transforming ingested json")

  //  var b = AnalysisRunner.onData()

  var analysers = DeequSeq.AnalyzerArr(dataCollected)
  var b = DeequSeq.AnalyzerSeq(analysers)

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

  //  Check(CheckLevel.Error,"deded").areComplete(Seq("njdej"))

  renamedData
    .writeStream
    //        .outputMode("update")
    //        .trigger(Trigger.Once())
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      val stateStoreCurr = stateStoreNext

      // run our analysis on the current batch, aggregate with saved state
      val metricsResult = AnalysisRunner.run(
        data = batchDF,
        analysis = b,
        aggregateWith = Some(stateStoreCurr),
        saveStatesWith = Some(stateStoreNext))

        val verificationResult = VerificationSuite()
          .onData(batchDF)
          .addChecks(
            Seq(
              Check(CheckLevel.Error, "objectClass check")
                .isContainedIn("object_class", DataArrays.object_classArray)
              ,
              Check(CheckLevel.Error, "agreement number check")
                .areComplete(Seq("agreement_number"))
            )
          )
          .run()

      //      val verificationResult = VerificationSuite()
      //        .onData(batchDF)
      //        .addCheck(
      //          Check(CheckLevel.Error, "objectClass check")
      //            .isContainedIn("object_class", DataArrays.object_classArray)
      //        )
      //        .addCheck(
      //          Check(CheckLevel.Error, "agreementNumber check")
      //            .areComplete(Seq("agreement_number"))
      //        )
      //        .run()

      val x = checkResultsAsDataFrame(spark, verificationResult)

      if (verificationResult.status != CheckStatus.Success) {
        x.write.format("parquet").mode("overwrite").saveAsTable("bad_records")
        x.show()
      }

      val metric_results = successMetricsAsDataFrame(spark, metricsResult)
        .withColumn("ts", current_timestamp())

      metric_results.show()

      metric_results.write.format("parquet").mode("Overwrite").saveAsTable("deequ_metrics")

      //          Main.main()
      println("back in streaming")

    }
    .start()
    .awaitTermination()
}
