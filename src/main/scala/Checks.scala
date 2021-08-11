import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}

object Checks {

  var splitSeq = Seq[String]()
  var i = 0
  var j = 0

  def finalCheck(seq: Seq[(String,String,String,Seq[String])]) = {
    var checkSeq = Seq[Check]()
    seq.foreach{
      case (checkName,"hasSize",columnName,params) => {
        checkSeq = checkSeq :+ Check(CheckLevel.Error,checkName)
          .hasSize(_ >= params.head.toInt)
      }
      case (checkName,"isComplete",columnName,null) => {
        checkSeq = checkSeq :+ Check(CheckLevel.Error,checkName)
          .isComplete(columnName)
      }
      case _ => "invalid"
    }
    checkSeq

  }

  def ChecksSeq(x: Array[org.apache.spark.sql.Row]) = {
    //check name //function // column name //params
    var seq = Seq[(String,String,String,Seq[String])]()
    for (i <- Range(0,x.length)){
      if (x(i).get(3) == null){
        seq = seq :+ (x(i).get(0).toString,x(i).get(1).toString,x(i).get(2).toString,null)
      }
      else {
        var param = x(i).get(3).toString.substring(1, x(i).get(3).toString.length()-1)
        // removing the [] from the string and in the next line removing the "
        param = param.replace("\"", "")
        seq = seq :+ (x(i).get(0).toString,x(i).get(1).toString,x(i).get(2).toString,HelperFunctions.stringToSeq(param))
      }
    }
    finalCheck(seq)
  }
}
