package io.ddf.flink.etl

import io.ddf.etl.IHandleMissingData.{Axis, NAChecking}
import io.ddf.exception.DDFException
import io.ddf.flink.BaseSpec

class MissingDataHandlerSpec extends BaseSpec {

  val missingData = flinkDDFManager.loadTable(getClass.getResource("/airlineWithNA.csv").getPath, ",")

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

  /*it should "fill by value" in {
    val ddf1: DDF = missingData.VIEWS.project(List("V1", "V17", "V28", "V29"))

    val filledDDF: DDF = ddf1.fillNA("0")
    val annualDelay = filledDDF.aggregate("V1, sum(V29)").get("2008")(0)
    annualDelay should be(282.0 +- 0.1)
  }*/

}
