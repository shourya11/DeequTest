import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}

object Checks {

  var seq = Seq[(String,String,String,String)]()
  var splitSeq = Seq[String]()
  var i = 0
  var j = 0

  def finalCheck(seq: Seq[(String,String,String,String)]) = {
    //    var asd = Seq[Check]()
    //    val y =  Check(CheckLevel.Error, "objectClass check")
    //      .isContainedIn("object_class", DataArrays.object_classArray)
    //    val z = Check(CheckLevel.Error, "agreementNumber check")
    //      .areComplete(Seq("agreement_number"))
    //
    //    asd = asd :+ y :+ z
//    seq.foreach{
//
//    }
  }

  def ChecksSeq(x: Array[org.apache.spark.sql.Row]) = {
    //check name //function // column name //params
    for (i <- Range(0,x.length)){
      if (x(i).get(3) == null){
        seq = seq :+ (x(i).get(0).toString,x(i).get(1).toString,x(i).get(2).toString,null)
      }
      else {
        seq = seq :+ (x(i).get(0).toString,x(i).get(1).toString,x(i).get(2).toString,x(i).get(3).toString)
      }
    }
    seq
  }
}
