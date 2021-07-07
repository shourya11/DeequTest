import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.{Completeness, Compliance, CountDistinct, MaxLength, Maximum, Mean, MinLength, Minimum, Size, Sum}
import com.amazon.deequ.anomalydetection.RelativeRateOfChangeStrategy
import com.amazon.deequ.checks.CheckStatus.Success
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import org.apache.spark.sql.SparkSession

object Anomaly {


  val session : SparkSession = SparkSession.builder().getOrCreate()
  import session.implicits._
  val metricsRepository = new InMemoryMetricsRepository()

  val dfFromRDD1 = Seq(
    (1, "Thingy A", "awesome thing.", "high", 45),
    (2, "Thingy B", "available at http://thingb.com", null, 90)).toDF()

//  val rdd = session.sparkContext.parallelize(yesterdaysDataset)
//  val dfFromRDD1 = rdd.toDF()

  val yesterdaysKey = ResultKey(System.currentTimeMillis() - 24 * 60 * 60 * 1000)

  VerificationSuite()
    .onData(dfFromRDD1)
    .useRepository(metricsRepository)
    .saveOrAppendResult(yesterdaysKey)
    .addAnomalyCheck(
      RelativeRateOfChangeStrategy(maxRateIncrease = Some(4.0)),
      Size())
    .addAnomalyCheck(
      RelativeRateOfChangeStrategy(maxRateIncrease = Some(10.0)),
      Minimum("_5")
    )
    .addAnomalyCheck(
      RelativeRateOfChangeStrategy(maxRateIncrease = Some(5.0)),
      Sum("_5")
    )
    .addAnomalyCheck(
      RelativeRateOfChangeStrategy(maxRateIncrease = Some(1.0)),
      MaxLength("_2")
    )
    .addAnomalyCheck(
      RelativeRateOfChangeStrategy(maxRateIncrease = Some(1.0)),
      Compliance("_5","_5 >= 999.0")
    )
    .run()


  val dfFromRDD2 = Seq(
    (1, "Thingy A", "awesome thing.", "high", 356),
    (2, "Thingy B", "available at http://thingb.com", null, 346),
    (3, null, null, "low", 346),
    (4, "Thingy D", "checkout https://thingd.ca", "low", 345),
    (5, "Thingy EFD", null, "high", 345)).toDF()

//  val rdd2 = session.sparkContext.parallelize(todaysDataset)
//  val dfFromRDD2 = rdd2.toDF()


  val todaysKey = ResultKey(System.currentTimeMillis())

  val verificationResult = VerificationSuite()
    .onData(dfFromRDD2)
    .useRepository(metricsRepository)
    .saveOrAppendResult(todaysKey)
    .addAnomalyCheck(
      RelativeRateOfChangeStrategy(maxRateIncrease = Some(4.0)),
      Size())
    .addAnomalyCheck(
      RelativeRateOfChangeStrategy(maxRateIncrease = Some(10.0)),
      Minimum("_5")
    )
    .addAnomalyCheck(
      RelativeRateOfChangeStrategy(maxRateIncrease = Some(5.0)),
      Sum("_5")
    )
    .addAnomalyCheck(
      RelativeRateOfChangeStrategy(maxRateIncrease = Some(1.0)),
      MaxLength("_2")
    )
    .addAnomalyCheck(
      RelativeRateOfChangeStrategy(maxRateIncrease = Some(1.0)),
      Compliance("_5","_5 >= 999.0") // returns 0 when false and 1 when true in value column
    )
    .run()

    print(verificationResult)
    println("")

  if (verificationResult.status != Success) {
    println("Anomaly detected")

    metricsRepository
      .load()
      .forAnalyzers(Seq(Size(),Minimum("_5"),Sum("_5"),MaxLength("_2"),Compliance("_5","_5 >= 999.0")))
      .getSuccessMetricsAsDataFrame(session)
      .show()
  }
  else {
    print("no detections in the data")
    println("")
  }
}

