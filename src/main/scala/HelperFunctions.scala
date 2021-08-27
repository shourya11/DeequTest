import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import java.text.SimpleDateFormat
import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

object HelperFunctions {
  val spark = SparkSession.builder().getOrCreate()

  def stringToSeq(s: String) = {
    var splitSeq = Seq[String]()
    var x = s.split(",")
    for (i <- Range(0, x.length)) {
      splitSeq = splitSeq :+ x(i)
    }
    splitSeq
  }

  def stringToMap(s: String) = {
    s.substring(1, s.length - 1)
      .split(",")
      .map(_.split(":"))
      .map { case Array(k, v) => (k.substring(1, k.length - 1), v.substring(1, v.length - 1)) }
      .toMap
  }


  def FileName: ((String) => String) = { (s) => (s.split("/").reverse(0)) }

  val myFileName = udf(FileName)

  val getTimestampWithMilis: ((String, String) => Option[Timestamp]) = (input, frmt) => input match {
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
