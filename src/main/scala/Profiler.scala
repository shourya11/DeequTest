import com.amazon.deequ.profiles.{ColumnProfilerRunner, NumericColumnProfile}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, StatefulApproxQuantile, StatefulHyperloglogPlus}

//
object Profiler {
  val spark: SparkSession = SparkSession.builder().getOrCreate()

  def profiling(incomingData: DataFrame): Unit = {
    val result = ColumnProfilerRunner().onData(incomingData).run()

    result.profiles.foreach { case (colName, profile) =>
      println(s"Column '$colName':\n " +
        s"\tcompleteness: ${profile.completeness}\n" +
        s"\tapproximate number of distinct values: ${profile.approximateNumDistinctValues}\n" +
        s"\tdatatype: ${profile.dataType}\n")
    }
  }
}
