import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.analyzers.{Completeness, Correlation, Distinctness, Size}
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Deequ {
  val spark : SparkSession = SparkSession.builder().getOrCreate()
  def analyser(renamedData: DataFrame): DataFrame = {
    val analysis: AnalyzerContext = {
      AnalysisRunner.onData(renamedData)
        .addAnalyzer(Size())
        .addAnalyzer(Distinctness("object_class"))
        .addAnalyzer(Completeness("object_class"))
        .addAnalyzer(Completeness("agreement_number"))
        .addAnalyzer(Completeness("sysAudit_object_class"))
        .run()
    }
    successMetricsAsDataFrame(spark, analysis)

  }

  def verification(renamedData: DataFrame): DataFrame = {
    val verificationResult: VerificationResult = VerificationSuite()
      .onData(renamedData)
      .addCheck(
        Check(CheckLevel.Error, "objectClass check")
          .isContainedIn("object_class", DataArrays.object_classArray)
      )
      .run()

    checkResultsAsDataFrame(spark, verificationResult)
  }

  def verificationColumns(dfname: String, renamedData: DataFrame, columns: Seq[String]) = {
    val verificationResult: VerificationResult = VerificationSuite()
      .onData(renamedData)
      .addCheck(
          Check(CheckLevel.Error, dfname + "Columns check")
            .areComplete(columns)
      )
      .run()
    checkResultsAsDataFrame(spark, verificationResult)

  }
}
