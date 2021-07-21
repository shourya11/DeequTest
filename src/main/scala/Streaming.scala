import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.{col, current_timestamp, input_file_name, lit}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.{Analysis, ApproxCountDistinct, Completeness, Compliance, Distinctness, InMemoryStateProvider, MaxLength, Size}

object Streaming {
  val spark : SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

//  def main(): Unit = {

    var base_df = spark.read.schema(SchemaData.jsonSourceSchema).format("json").load("C:\\Users\\shour\\Desktop\\Whiteklay\\data\\*.json")
    val empty_df = base_df.where("0 = 1")
    val l1: Long = 0

    spark.sql("DROP TABLE IF EXISTS trades_delta")
    spark.sql("DROP TABLE IF EXISTS bad_records")
    spark.sql("DROP TABLE IF EXISTS deequ_metrics")

    base_df.createOrReplaceTempView("trades_historical")
    empty_df.write.format("parquet").saveAsTable("trades_delta")
    empty_df.withColumn("batchID",lit(l1)).write.format("parquet").saveAsTable("bad_records")

    val analysis = Analysis()
      .addAnalyzer(Size())
      .addAnalyzer(Completeness("object_class"))
      .addAnalyzer(Completeness("agreement_number"))
      .addAnalyzer(Completeness("sysAudit_object_class"))
      .addAnalyzer(MaxLength("object_class"))


    val stateStoreCurr = InMemoryStateProvider()
    val stateStoreNext = InMemoryStateProvider()

//  val x = spark.readStream
//    .schema(SchemaData.jsonSourceSchema)
//    .format("json")
//    .load("C:\\Users\\shour\\Desktop\\Whiteklay\\data\\*.json")
//
//    RenameData.dataRenamed(x)
//      .repartition(1)
//      .writeStream.format("parquet")
//      .option("failOnDataLoss", "false")
//      .option("checkpointLocation", "/checkpoint")
//      .format("parquet")
//      .outputMode("append")
//      .trigger(Trigger.Once())
//      .option("path","C:\\Users\\shour\\Desktop\\Whiteklay\\tables")
//      .start()
//      .awaitTermination()

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
            analysis = analysis,
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


//  val batchCounts = spark.read.format("parquet").table("bad_records")
//    .groupBy($"batchId").count()
//  batchCounts.printSchema()

}



