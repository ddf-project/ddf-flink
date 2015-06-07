package io.ddf.flink.analytics

import io.ddf.DDF
import io.ddf.content.Schema.ColumnClass
import io.ddf.flink.BaseSpec
import org.apache.flink.api.scala.DataSet
import org.scalatest.Matchers

import scala.collection.JavaConverters._

class BinningHandlerSpec extends BaseSpec {
  it should "bin the ddf" in {
    val ddf = loadAirlineDDF()
    val newDDF: DDF = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, true)

    newDDF.getSchemaHandler.getColumn("dayofweek").getColumnClass should be(ColumnClass.FACTOR)

    newDDF.getSchemaHandler.getColumn("dayofweek").getOptionalFactor.getLevelMap.size should be(2)

    val ddf1: DDF = ddf.binning("month", "custom", 0, Array[Double](2, 4, 6, 8), true, true)

    ddf1.getSchemaHandler.getColumn("month").getColumnClass should be(ColumnClass.FACTOR)
    // {'[2,4]'=1, '(4,6]'=2, '(6,8]'=3}
    ddf1.getSchemaHandler.getColumn("month").getOptionalFactor.getLevelMap.get("[2,4]") should be(1)

  }
}
