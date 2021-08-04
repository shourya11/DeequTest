import com.amazon.deequ.analyzers.{Analysis, Completeness, Compliance, MaxLength, Size}

object DeequSeq {

  var arr = Array[(Any,Any,Any)]()
  var b = Analysis()
  var i = 0
  var j = 0

  def AnalyzerSeq (analysers :Array[(Any,Any,Any)]) = {
    analysers.foreach{
      case ("Size",null,null) => {
        b = b.addAnalyzer(Size())
      }
      case("Completeness",str,null) => {
        b  = b.addAnalyzer(Completeness(str.toString))
      }
      case("MaxLength",str,null) => {
        b  = b.addAnalyzer(MaxLength(str.toString))
      }
      case("Compliance",str,str2) => {
        b  = b.addAnalyzer(Compliance(str.toString,str2.toString))
      }
      case _ => "invalid"
    }
    b
  }


  def AnalyzerArr(x: Array[org.apache.spark.sql.Row]) = {
    for (i <- Range(0,x.length)){
      if (x(i).get(1) == null){
        arr = arr :+ (x(i).get(0).toString,null,null)

      }
      else if (x(i).get(2) == null ){
        arr = arr :+ (x(i).get(0).toString,x(i).get(1).toString,null)
      }
      else {
        arr = arr :+ (x(i).get(0).toString,x(i).get(1).toString,x(i).get(2).toString)
      }
    }
    arr
  }
}
