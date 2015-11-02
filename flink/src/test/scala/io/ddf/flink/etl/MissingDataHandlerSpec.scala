package io.ddf.flink.etl

import io.ddf.DDF
import io.ddf.etl.IHandleMissingData.{NAChecking, Axis}
import io.ddf.exception.DDFException
import io.ddf.flink.BaseSpec
import io.ddf.types.AggregateTypes.AggregateFunction
import org.apache.flink.api.scala.DataSet

import scala.collection.JavaConversions._

class MissingDataHandlerSpec extends BaseSpec {

  val missingData = loadAirlineNADDF()

  it should "drop all rows with NA values" in {
    val result = missingData.dropNA()
    result.getNumRows should be(9)
  }

  it should "keep all the rows" in {
    val result = missingData.getMissingDataHandler.dropNA(Axis.ROW, NAChecking.ALL, 0, null)
    result.getNumRows should be(31)
  }

  it should "keep all the rows when drop threshold is high" in {
    val result = missingData.getMissingDataHandler.dropNA(Axis.ROW, NAChecking.ALL, 10, null)
    result.getNumRows should be(31)
  }

  it should "throw an exception when drop threshold > columns" in {
    intercept[DDFException] {
      missingData.getMissingDataHandler.dropNA(Axis.ROW, NAChecking.ANY, 31, null)
    }
  }

  it should "drop all columns with NA values" in {
    val result = missingData.dropNA(Axis.COLUMN)
    result.getNumColumns should be(22)
  }

  it should "drop all columns with NA values with load table" in {
    val missingData = loadAirlineNADDF()
    val result = missingData.dropNA(Axis.COLUMN)
    result.getNumColumns should be(22)
  }

  it should "keep all the columns" in {
    val result = missingData.getMissingDataHandler.dropNA(Axis.COLUMN, NAChecking.ALL, 0, null)
    result.getNumColumns should be(29)
  }

  it should "keep most(24) columns when drop threshold is high(20)" in {
    val result = missingData.getMissingDataHandler.dropNA(Axis.COLUMN, NAChecking.ALL, 20, null)
    result.getNumColumns should be(24)
  }

  it should "throw an exception when drop threshold > rows" in {
    intercept[DDFException] {
      missingData.getMissingDataHandler.dropNA(Axis.COLUMN, NAChecking.ANY, 40, null)
    }
  }

  it should "fill by value" in {
    val ddf = loadDDF()
    val ddf1: DDF = ddf.VIEWS.project(List("V1", "V29"))
    val filledDDF: DDF = ddf1.fillNA("0")
    val annualDelay = filledDDF.aggregate("V1, sum(V29)").get("2008")(0)
    annualDelay should be(282.0 +- 0.1)
  }

  it should "fill by dictionary" in {
    val ddf = loadDDF()
    val ddf1: DDF = ddf.VIEWS.project(List("V1", "V28", "V29"))
    val dict: Map[String, String] = Map("V1" -> "2000", "V28" -> "0", "V29" -> "1")
    val filledDDF = ddf1.getMissingDataHandler.fillNA(null, null, 0, null, dict, null)
    val annualDelay = filledDDF.aggregate("V1, sum(V29)").get("2008")(0)
    annualDelay should be(282.0 +- 0.1)
  }

  it should "fill by aggregate function" in {
    val ddf = loadDDF()
    val ddf1: DDF = ddf.VIEWS.project(List("V1", "V28", "V29"))
    val result = ddf1.getMissingDataHandler.fillNA(null, null, 0, AggregateFunction.MEAN, null, null)
    result should not be (null)
  }

}
