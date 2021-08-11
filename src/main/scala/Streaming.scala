import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.{col, current_timestamp, input_file_name, lit}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers._

object Streaming {
  val spark : SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  def run(Format:String,Path:String,analysers:Analysis,checks:Seq[Check]): Unit = {

    var base_df = spark.read.schema(SchemaData.jsonSourceSchema).format(Format).load(Path)
    val empty_df = base_df.where("0 = 1")
    val l1: Long = 0

    spark.sql("DROP TABLE IF EXISTS trades_delta")
    spark.sql("DROP TABLE IF EXISTS bad_records")
    spark.sql("DROP TABLE IF EXISTS deequ_metrics")

    base_df.createOrReplaceTempView("trades_historical")
    empty_df.write.format("parquet").saveAsTable("trades_delta")
    empty_df.withColumn("batchID", lit(l1)).write.format("parquet").saveAsTable("bad_records")

    val stateStoreCurr = InMemoryStateProvider()
    val stateStoreNext = InMemoryStateProvider()

    println("reading data")

    val original_data = spark.readStream
      .schema(SchemaData.jsonSourceSchema)
      //    .option("maxFilesPerTrigger",20)
      .format(Format)
      .load(Path)

    val renamedData = RenameData.dataRenamed(original_data)

    renamedData
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
          .run()

        val x = checkResultsAsDataFrame(spark, verificationResult)

        if (verificationResult.status != CheckStatus.Success) {
          x.write.format("parquet").mode("overwrite").saveAsTable("bad_records")
          x.show()
        }

        val metric_results = successMetricsAsDataFrame(spark, metricsResult)
          .withColumn("ts", current_timestamp())

        metric_results.show()

        metric_results.write.format("parquet").mode("Overwrite").saveAsTable("deequ_metrics")

        println("back in streaming")

      }
      .start()
      .awaitTermination()

    //  val batchCounts = spark.read.format("parquet").table("bad_records")
    //    .groupBy($"batchId").count()
    //  batchCounts.printSchema()


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

  }
}



