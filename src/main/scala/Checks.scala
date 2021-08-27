import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.ConstrainableDataTypes

//convert params to map minValue and maxValue done
//passing the json done
//convert to jar - jdk 1.8
//cloud functions firing up a spark job
//web ui

object Checks {

  var splitSeq = Seq[String]()
  var i = 0
  var j = 0

  def finalCheck(seq: Seq[(String, String, String, Map[String, String], Seq[String])]) = {
    var checkSeq = Seq[Check]()
    seq.foreach {
      case (checkName, "hasSize", null, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasSize(_ >= params("minValue").toInt)

      case (checkName, "isPrimaryKey", columnName, null, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .isPrimaryKey(columnName)

      case (checkName, "isComplete", columnName, null, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .isComplete(columnName)

      case (checkName, "hasCompleteness", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasCompleteness(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "areComplete", null, null, multipleColumns) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .areComplete(multipleColumns)

      case (checkName, "haveCompleteness", null, params, multipleColumns) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .haveCompleteness(multipleColumns, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "areAnyComplete", null, null, multipleColumns) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .areAnyComplete(multipleColumns)

      case (checkName, "haveAnyCompleteness", null, params, multipleColumns) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .haveAnyCompleteness(multipleColumns, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "isUnique", columnName, null, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .isUnique(columnName)

      case (checkName, "hasDistinctness", null, params, multipleColumns) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasDistinctness(multipleColumns, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasUniqueness", null, params, multipleColumns) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasUniqueness(multipleColumns, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasUniqueness", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasUniqueness(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasEntropy", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasEntropy(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasMutualInformation", null, params, multipleColumns) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasMutualInformation(multipleColumns(0), multipleColumns(1), v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasApproxQuantile", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasApproxQuantile(columnName, params("doubleValue").toDouble, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasMinLength", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasMinLength(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasMaxLength", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasMaxLength(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasMin", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasMin(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasMax", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasMax(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasMean", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasMean(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasSum", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasSum(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasStandardDeviation", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasStandardDeviation(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasApproxCountDistinct", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasApproxCountDistinct(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasCorrelation", null, params, multipleColumns) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasCorrelation(multipleColumns(0), multipleColumns(1), v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasPattern", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasPattern(columnName, params("pattern").r(), v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "containsCreditCardNumber", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .containsCreditCardNumber(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "containsEmail", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .containsEmail(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "containsURL", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .containsURL(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "containsSocialSecurityNumber", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .containsSocialSecurityNumber(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "hasDataType", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .hasDataType(columnName, ("ConstrainableDataTypes." + params("DataType")).asInstanceOf[ConstrainableDataTypes.Value], v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "isNonNegative", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .isNonNegative(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "isPositive", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .isPositive(columnName, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "isLessThan", null, params, multipleColumns) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .isLessThan(multipleColumns(0), multipleColumns(1), v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "isLessThanOrEqualTo", null, params, multipleColumns) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .isLessThanOrEqualTo(multipleColumns(0), multipleColumns(1), v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "isGreaterThan", null, params, multipleColumns) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .isGreaterThan(multipleColumns(0), multipleColumns(1), v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "isGreaterThanOrEqualTo", null, params, multipleColumns) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .isGreaterThanOrEqualTo(multipleColumns(0), multipleColumns(1), v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "isContainedIn", columnName, null, multipleColumns) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .isContainedIn(columnName, multipleColumns.toArray)

      case (checkName, "isContainedIn", columnName, params, multipleColumns) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .isContainedIn(columnName, multipleColumns.toArray, v => v > params("minValue").toInt && v < params("maxValue").toInt)

      case (checkName, "isContainedIn", columnName, params, null) =>
        checkSeq = checkSeq :+ Check(CheckLevel.Error, checkName)
          .isContainedIn(columnName, params("lowerBound").toDouble, params("upperBound").toDouble, params("includeLowerBound").toBoolean, params("includeUpperBound").toBoolean)

      case _ => "invalid"
    }

    checkSeq

  }


  def ChecksSeq(x: Array[org.apache.spark.sql.Row]) = {
    //check name //function // column name //params // multipleColumns
    var seq = Seq[(String, String, String, Map[String, String], Seq[String])]()
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
        val params = HelperFunctions.stringToMap(x(i).get(3).toString)
        // removing the [] from the string and in the next line removing the "
        // params = params.replace("\"", "") to remove " from the chars

        if (x(i).get(4) == null) {
          if (x(i).get(2) == null) {
            seq = seq :+ (x(i).get(0).toString, x(i).get(1).toString, null, params, null)
          }
          else {
            seq = seq :+ (x(i).get(0).toString, x(i).get(1).toString, x(i).get(2).toString, params, null)
          }
        }
        else {
          var mcolumns = x(i).get(4).toString.substring(1, x(i).get(4).toString.length()-1)
          mcolumns = mcolumns.replace("\"", "")
          val mcolumnsSeq = HelperFunctions.stringToSeq(mcolumns)


            if (x(i).get(2) == null) {
              seq = seq :+ (x(i).get(0).toString, x(i).get(1).toString, null, params, mcolumnsSeq)
            }
            else {
              seq = seq :+ (x(i).get(0).toString, x(i).get(1).toString, x(i).get(2).toString, params, mcolumnsSeq)
            }
        }
      }
    }

    finalCheck(seq)

  }
}
