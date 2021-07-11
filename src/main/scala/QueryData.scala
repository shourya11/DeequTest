import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, to_json, udf}

import java.text.SimpleDateFormat
import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

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

  def move_files(fileName:Array[String], fromLocation:String, toLocation:String): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    try {
      for (e <- fileName.indices){
        var file_source = new Path(fromLocation + "/" + fileName(e))

        var file_target = new Path(toLocation + fileName(e))
        fs.rename(file_source, file_target)
      }
    } catch {
      case e: Exception => println(e); println("Exception moving files between folders")
    }
  }

}
