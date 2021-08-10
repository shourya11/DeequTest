import com.amazon.deequ.analyzers._

object Analyzers {

  var arr = Array[(String,String,String)]()
  var splitSeq = Seq[String]()
  var i = 0
  var j = 0

  def stringToSeq(s: String) = {
    var x = s.split(",")
    for (i <- Range(0,x.length)){
      splitSeq = splitSeq :+ x(i)
    }
    splitSeq
  }

// for one column put it in default case and for multiple column taking put it in specific swtiches
  def finalAnalyzer (analyser :Array[(String,String,String)]) = {
    var b = Analysis()
    analyser.foreach{
      case ("Size",null,null) => {
        b = b.addAnalyzer(Size())
      }
      case("Completeness",str,null) => {
        b  = b.addAnalyzer(Completeness(str))
      }
      case("MaxLength",str,null) => {
        b  = b.addAnalyzer(MaxLength(str))
      }
      case("MinLength",str,null) => {
        b  = b.addAnalyzer(MinLength(str))
      }
      case("Compliance",str,str2) => {
        b  = b.addAnalyzer(Compliance(str,str2))
      }
      case("Mean",str,null) => {
        b  = b.addAnalyzer(Mean(str))
      }
      case("Maximum",str,null) => {
        b  = b.addAnalyzer(Maximum(str))
      }
      case("Minimum",str,null) => {
        b  = b.addAnalyzer(Minimum(str))
      }
      case("PatternMatch",str,reg) => {
        b  = b.addAnalyzer(PatternMatch(str,reg.r))
      }
      case("StandardDeviation",str,null) => {
        b  = b.addAnalyzer(StandardDeviation(str))
      }
      case("Sum",str,null) => {
        b  = b.addAnalyzer(Sum(str))
      }
      case("UniqueValueRatio",str,null) => {
        b  = b.addAnalyzer(UniqueValueRatio(str))
      }
      case("Uniqueness",str,null) => {
        b  = b.addAnalyzer(Uniqueness(str))
      }
      case("MutualInformation",str,null) => {
        var str2 = str.substring(1, str.length()-1)
        str2 = str2.replace("\"", "")
        b  = b.addAnalyzer(MutualInformation(stringToSeq(str2)))
      }
      case("Distinctness",str,null) => {
        b  = b.addAnalyzer(Distinctness(str))
      }
      case("CountDistinct",str,null) => {
        b  = b.addAnalyzer(CountDistinct(str))
      }
      case("Correlation",str,str2) => {
        b  = b.addAnalyzer(Correlation(str,str2))
      }
      case("ApproxCountDistinct",str,null) => {
        b  = b.addAnalyzer(ApproxCountDistinct(str))
      }
      case("ApproxQuantile",str,str2) => {
        b  = b.addAnalyzer(ApproxQuantile(str,str2.toDouble))
      }
      case("DataType",str,null) => {
        b = b.addAnalyzer(DataType(str))
      }
      case("Entropy",str,null) => {
        b = b.addAnalyzer(Entropy(str))
      }
      case("KLLSketch",str,null) => {
        b = b.addAnalyzer(KLLSketch(str))
      }
      case("Histogram",str,null) => {
        b = b.addAnalyzer(Histogram(str))
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
    finalAnalyzer(arr)
  }

}
