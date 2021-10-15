import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.{col, current_timestamp, input_file_name, lit}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers._
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import java.io._

object Streaming {

  val spark: SparkSession = SparkSession.builder().getOrCreate()
  val metricsFile = new File("C:\\Users\\shour\\Desktop\\Whiteklay\\metrics.json")
  val repository = FileSystemMetricsRepository(spark, metricsFile.getAbsolutePath)
  val resultKey = ResultKey(
    System.currentTimeMillis(),
    Map("tag" -> "repositoryExample")
  )

  def run(InputFormat: String, InputPath: String, DestFormat: String, DestPath: String, DestMode: String, analysers: Analysis, checks: Seq[Check]): Unit = {

    var base_df = spark.read.schema(SchemaData.jsonSourceSchema).format(InputFormat).load(InputPath)
    val empty_df = base_df.where("0 = 1")
    val l1: Long = 0

    spark.sql("DROP TABLE IF EXISTS checks_table")
    spark.sql("DROP TABLE IF EXISTS analysers_table")

    empty_df.withColumn("batchID", lit(l1)).write.format(DestFormat).saveAsTable("checks_table")

    val stateStoreCurr = InMemoryStateProvider()
    val stateStoreNext = InMemoryStateProvider()

    println("reading data")
    val original_data = spark.readStream
      .schema(SchemaData.jsonSourceSchema)
      //      .option("maxFilesPerTrigger",20)
      .format(InputFormat)
      .load(InputPath)

    val renamedData = RenameData.dataRenamed(original_data)

    renamedData
      //      .withWatermark("","")
      .writeStream
      //        .outputMode("update")
      .trigger(Trigger.Once())
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val stateStoreCurr = stateStoreNext

        // run our analysis on the current batch, aggregate with saved state
        val metricsResult = AnalysisRunner.run(
          data = batchDF,
          analysis = analysers,
          aggregateWith = Some(stateStoreCurr),
          saveStatesWith = Some(stateStoreNext))

        val verificationResult = VerificationSuite()
          .onData(batchDF)
          .addChecks(
            checks
          )
          .useRepository(repository)
          .saveOrAppendResult(resultKey)
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
          //          x.write.format(DestFormat).mode(DestMode).saveAsTable("checks_table")
          //showing verification results
          x.show()
        }

        val metric_results = successMetricsAsDataFrame(spark, metricsResult)
          .withColumn("ts", current_timestamp())
        //showing analysis results
        metric_results.show()
        //        metric_results.write.format(DestFormat).mode(DestMode).saveAsTable("analysers_table")
      }
      .start()
      .awaitTermination()

    //Reading the metric json after the streaming is complete
    val completenessOfProductName = repository
      .loadByKey(resultKey).get
      .metric(Completeness("agreement_number")).get

    println(s"The completeness of the agreementNumber column is: $completenessOfProductName")

    repository.load()
      .withTagValues(Map("tag" -> "repositoryExample"))
      .getSuccessMetricsAsDataFrame(spark)
      .show()

  }
}





