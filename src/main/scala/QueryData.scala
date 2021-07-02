import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{to_json, udf}
import java.text.SimpleDateFormat
import java.sql.Timestamp
import scala.util.{Try, Success, Failure}

object QueryData {
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  def dataFilter(df : DataFrame, obj: Array[String]): DataFrame = {
    df.filter($"object_class".isin(obj:_*))
      .withColumn("payload",to_json($"payload"))
  }

  def FileName: ((String) => String) = { (s) =>(s.split("/").reverse(0))}
  val myFileName = udf(FileName)

  val coder2: (String => String) = (arg: String) => {"Incorrect or Missing ObjectClass"}
  val inc2 = udf(coder2)

  val getTimestampWithMilis: ((String , String) => Option[Timestamp]) = (input, frmt) => input match {
    case "" => None
    case _ => {
      val format = new SimpleDateFormat(frmt)
      Try(new Timestamp(format.parse(input).getTime)) match {
        case Success(t) => Some(t)
        case Failure(_) => None
      }
    }
  }

  val getTimestampWithMilisUDF = udf(getTimestampWithMilis)

}
