import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}

object Checks {

  var splitSeq = Seq[String]()
  var i = 0
  var j = 0

  def finalCheck(seq: Seq[(String,String,String,Seq[String],Seq[String])]) = {
    var checkSeq = Seq[Check]()
    seq.foreach{
      case (checkName,"hasSize",columnName,params,multipleColumns) => {
        checkSeq = checkSeq :+ Check(CheckLevel.Error,checkName)
          .hasSize(_ >= params.head.toInt)
      }
      case (checkName,"isPrimaryKey",columnName,params,multipleColumns) => {
        checkSeq = checkSeq :+ Check(CheckLevel.Error,checkName)
          .isPrimaryKey(columnName)
      }
      case (checkName,"isComplete",columnName,params,multipleColumns) => {
        checkSeq = checkSeq :+ Check(CheckLevel.Error,checkName)
          .isComplete(columnName)
      }
      case (checkName,"hasDistinctness",columnName,params,multipleColumns) => {
        checkSeq = checkSeq :+ Check(CheckLevel.Error,checkName)
          .isComplete(columnName)
      }
      case (checkName,"hasUniqueness",columnName,params,multipleColumns) => {
        checkSeq = checkSeq :+ Check(CheckLevel.Error,checkName)
          .hasUniqueness(multipleColumns,v => v > params(0).toInt && v < params(1).toInt)
      }
      case (checkName,"hasUniqueness",columnName,params,null) => {
        checkSeq = checkSeq :+ Check(CheckLevel.Error,checkName)
          .hasUniqueness(columnName,v => v > params(0).toInt && v < params(1).toInt)
      }
      case (checkName,"hasEntropy",columnName,params,multipleColumns) => {
        checkSeq = checkSeq :+ Check(CheckLevel.Error,checkName)
          .hasEntropy(columnName,v => v > params(0).toInt && v < params(1).toInt)
      }
      case (checkName,"hasMutualInformation",columnName,params,multipleColumns) => {
        checkSeq = checkSeq :+ Check(CheckLevel.Error,checkName)
          .hasMutualInformation(multipleColumns(0),multipleColumns(1),v => v > params(0).toInt && v < params(1).toInt)
      }
      case (checkName,"hasApproxQuantile",columnName,params,multipleColumns) => {
        checkSeq = checkSeq :+ Check(CheckLevel.Error,checkName)
          .hasApproxQuantile(columnName,params(0).toDouble,v => v > params(1).toInt && v < params(2).toInt)
      }
      case (checkName,"hasMinLength",columnName,params,multipleColumns) => {
        checkSeq = checkSeq :+ Check(CheckLevel.Error,checkName)
          .hasMinLength(columnName,v => v > params(0).toInt && v < params(1).toInt)
      }
      case (checkName,"hasMaxLength",columnName,params,multipleColumns) => {
        checkSeq = checkSeq :+ Check(CheckLevel.Error,checkName)
          .hasMaxLength(columnName,v => v > params(0).toInt && v < params(1).toInt)
      }
      case _ => "invalid"
    }
    checkSeq

  }

  def ChecksSeq(x: Array[org.apache.spark.sql.Row]) = {
    //check name //function // column name //params
    var seq = Seq[(String,String,String,Seq[String],Seq[String])]()
    for (i <- Range(0,x.length)) {

      if (x(i).get(3) == null){

        if (x(i).get(4) == null)
        {
          seq = seq :+ (x(i).get(0).toString,x(i).get(1).toString,x(i).get(2).toString,null,null)
        }

        else {
          val mcolumns = x(i).get(4).toString.substring(1, x(i).get(4).toString.length()-1)
          val mcolumnsSeq = HelperFunctions.stringToSeq(mcolumns)

          if (x(i).get(2) == null)
          {
            seq = seq :+ (x(i).get(0).toString,x(i).get(1).toString,null,null,mcolumnsSeq)
          }

          else {
            seq = seq :+ (x(i).get(0).toString, x(i).get(1).toString, x(i).get(2).toString, null, mcolumnsSeq)
          }
        }
      }

      else {
        var param = x(i).get(3).toString.substring(1, x(i).get(3).toString.length()-1)
        // removing the [] from the string and in the next line removing the "
        param = param.replace("\"", "")

        if (x(i).get(4) == null) {
          seq = seq :+ (x(i).get(0).toString,x(i).get(1).toString,x(i).get(2).toString,HelperFunctions.stringToSeq(param),null)
        }

        else {
          val mcolumns = x(i).get(4).toString.substring(1, x(i).get(4).toString.length()-1)
          val mcolumnsSeq = HelperFunctions.stringToSeq(mcolumns)

          if (x(i).get(2) == null) {
            seq = seq :+ (x(i).get(0).toString, x(i).get(1).toString, null, HelperFunctions.stringToSeq(param), mcolumnsSeq)
          }

          else {
            seq = seq :+ (x(i).get(0).toString, x(i).get(1).toString, x(i).get(2).toString, HelperFunctions.stringToSeq(param), mcolumnsSeq)
          }
        }
      }
    }

    finalCheck(seq)

  }
}
