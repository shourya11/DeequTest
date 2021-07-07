//import org.apache.spark.sql.{SparkSession, _}
//import org.apache.spark.sql.streaming.Trigger
//import org.apache.spark.sql.functions.concat
//import org.apache.spark.sql.functions.{col, current_timestamp, lit}
//import org.apache.spark.sql.types.StringType
//import com.amazon.deequ.{VerificationSuite, VerificationResult}
//import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
//import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
//import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
//import com.amazon.deequ.analyzers._
//import com.amazon.deequ.analyzers.runners.AnalysisRunner
//import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
//import com.amazon.deequ.analyzers.{Analysis, ApproxCountDistinct, Completeness, Compliance, Distinctness, InMemoryStateProvider, Size}
//
//object Streaming {
//  val spark : SparkSession = SparkSession.builder().getOrCreate()
//  import spark.implicits._
//
//  def main(): Unit = {
//
//    val base_df = spark.read.schema(SchemaData.jsonSourceSchema).format("json").load("C:\\Users\\shour\\Desktop\\Whiteklay\\data\\*.json")
//    val empty_df = base_df.where("0 = 1")
//    val l1: Long = 0
//
//    spark.sql("DROP TABLE IF EXISTS trades_delta")
//    spark.sql("DROP TABLE IF EXISTS bad_records")
//    spark.sql("DROP TABLE IF EXISTS deequ_metrics")
//
//    base_df.createOrReplaceTempView("trades_historical")
//    empty_df.write.format("parquet").saveAsTable("trades_delta")
//    empty_df.withColumn("batchID",lit(l1)).write.format("parquet").saveAsTable("bad_records")
//
//    val analysis = Analysis()
//      .addAnalyzer(Size())
////      .addAnalyzer(Distinctness("object_class"))
//      .addAnalyzer(Completeness("object_class"))
//      .addAnalyzer(Completeness("agreement_number"))
//      .addAnalyzer(Completeness("sysAudit_object_class"))
//
//    val stateStoreCurr = InMemoryStateProvider()
//    val stateStoreNext = InMemoryStateProvider()
//
//  val x = spark.readStream
//    .schema(SchemaData.jsonSourceSchema)
//    .format("json")
////    .option("maxFilesPerTrigger",1)
//    .load("C:\\Users\\shour\\Desktop\\Whiteklay\\data\\*.json")
//
//    RenameData.dataRenamed(x)
//      .repartition(1)
//      .writeStream.format("parquet")
//      .option("failOnDataLoss", "false")
//      .option("checkpointLocation", "/checkpoint")
//      .outputMode("append")
//      .trigger(Trigger.Once())
//      .option("path","C:\\Users\\shour\\Desktop\\Whiteklay\\tables")
//      .start()
//      .awaitTermination()
//
////    spark.readStream
////      .format("orc")
////      .load("C:\\Users\\shour\\Desktop\\Whiteklay\\tables")
////      .writeStream
////      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
////
////        // reassign our current state to the previous next state
////        val stateStoreCurr = stateStoreNext
////
////        // run our analysis on the current batch, aggregate with saved state
////        val metricsResult = AnalysisRunner.run(
////          data = batchDF,
////          analysis = analysis,
////          aggregateWith = Some(stateStoreCurr),
////          saveStatesWith = Some(stateStoreNext))
////
////        // verify critical metrics for this microbatch i.e., trade quantity, ipaddr not null, etc.
////        val verificationResult = VerificationSuite()
////          .onData(batchDF)
////          .addCheck(
////            Check(CheckLevel.Error, "objectClass check")
////              .isContainedIn("object_class", DataArrays.object_classArray)
////          )
////          .run()
////
////        // if verification fails, write batch to bad records table
////        if (verificationResult.status != CheckStatus.Success) {
////          batchDF.withColumn("batchID",lit(batchId))
////            .write.format("delta").mode("append").saveAsTable("bad_records")
////        }
////
////        // get the current metrics as a dataframe
////        val metric_results = successMetricsAsDataFrame(spark, metricsResult)
////          .withColumn("ts", current_timestamp())
////
////        // write the current results into the metrics table
////        metric_results.write.format("delta").mode("Overwrite").saveAsTable("deequ_metrics")
////
////      }
////      .start()
//  }
//}
//
//
