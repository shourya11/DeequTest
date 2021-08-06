import com.amazon.deequ.analyzers._
import scala.util.matching.Regex

object DeequSeq {

  var arr = Array[(Any,Any,Any)]()
  var splitSeq = Seq[String]()
  var i = 0
  var j = 0

  def apply(s: String) = {
    var x = s.split(",")
    for (i <- Range(0,x.length)){
      splitSeq = splitSeq :+ x(i)
    }
    splitSeq
  }

  def AnalyzerSeq (analyser :Array[(Any,Any,Any)]) = {
    var b = Analysis()
    analyser.foreach{
      case ("Size",null,null) => {
        b = b.addAnalyzer(Size())
      }
      case("Completeness",str,null) => {
        b  = b.addAnalyzer(Completeness(str.toString))
      }
      case("MaxLength",str,null) => {
        b  = b.addAnalyzer(MaxLength(str.toString))
      }
      case("MinLength",str,null) => {
        b  = b.addAnalyzer(MinLength(str.toString))
      }
      case("Compliance",str,str2) => {
        b  = b.addAnalyzer(Compliance(str.toString,str2.toString))
      }
      case("Mean",str,null) => {
        b  = b.addAnalyzer(Mean(str.toString))
      }
      case("Maximum",str,null) => {
        b  = b.addAnalyzer(Maximum(str.toString))
      }
      case("Minimum",str,null) => {
        b  = b.addAnalyzer(Minimum(str.toString))
      }
      case("PatternMatch",str,reg) => {
        b  = b.addAnalyzer(PatternMatch(str.toString,reg.toString.r))
      }
      case("StandardDeviation",str,null) => {
        b  = b.addAnalyzer(StandardDeviation(str.toString))
      }
      case("Sum",str,null) => {
        b  = b.addAnalyzer(Sum(str.toString))
      }
      case("UniqueValueRatio",str,null) => {
        b  = b.addAnalyzer(UniqueValueRatio(str.toString))
      }
      case("Uniqueness",str,null) => {
        b  = b.addAnalyzer(Uniqueness(str.toString))
      }
      case("MutualInformation",str,null) => {
        var str2 = str.toString.substring(1, str.toString.length()-1)
        str2 = str2.replace("\"", "")
        b  = b.addAnalyzer(MutualInformation(apply(str2)))
      }
      case("Distinctness",str,null) => {
        b  = b.addAnalyzer(Distinctness(str.toString))
      }
      case("CountDistinct",str,null) => {
        b  = b.addAnalyzer(CountDistinct(str.toString))
      }
      case("Correlation",str,str2) => {
        b  = b.addAnalyzer(Correlation(str.toString,str2.toString))
      }
      case("ApproxCountDistinct",str,null) => {
        b  = b.addAnalyzer(ApproxCountDistinct(str.toString))
      }
      case("ApproxQuantile",str,str2) => {
        b  = b.addAnalyzer(ApproxQuantile(str.toString,str2.toString.toDouble))
      }
      case("DataType",str,null) => {
        b = b.addAnalyzer(DataType(str.toString))
      }
      case("Entropy",str,null) => {
        b = b.addAnalyzer(Entropy(str.toString))
      }
      case("KLLSketch",str,null) => {
        b = b.addAnalyzer(KLLSketch(str.toString))
      }
      case("Histogram",str,null) => {
        b = b.addAnalyzer(Histogram(str.toString))
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
