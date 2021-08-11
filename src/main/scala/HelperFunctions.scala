import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import java.text.SimpleDateFormat
import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

object HelperFunctions {
  val spark = SparkSession.builder().getOrCreate()
  var splitSeq = Seq[String]()

  def stringToSeq(s: String) = {
    var x = s.split(",")
    for (i <- Range(0,x.length)){
      splitSeq = splitSeq :+ x(i)
    }
    splitSeq
  }


  def FileName: ((String) => String) = { (s) =>(s.split("/").reverse(0))}
  val myFileName = udf(FileName)

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
