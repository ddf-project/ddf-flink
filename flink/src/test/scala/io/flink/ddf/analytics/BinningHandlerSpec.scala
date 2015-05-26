package io.flink.ddf.analytics

import io.ddf.{DDFManager, DDF}
import io.ddf.content.Schema.ColumnClass
import io.flink.ddf.BaseSpec
import io.flink.ddf.etl.SqlHandlerSpec
import org.apache.flink.api.scala.DataSet
import org.scalatest.Matchers
import scala.collection.JavaConverters._

class BinningHandlerSpec extends BaseSpec with Matchers {
  it should "bin the ddf" in {
    val ddf: DDF = loadAirlineDDF()
    val newDDF: DDF = ddf.binning("dayofweek", "EQUALINTERVAL", 2, null, true, true)

    newDDF.getSchemaHandler.getColumn("dayofweek").getColumnClass should be(ColumnClass.FACTOR)

    newDDF.getSchemaHandler.getColumn("dayofweek").getOptionalFactor.getLevelMap.size should be(2)

    val ddf1: DDF = ddf.binning("month", "custom", 0, Array[Double](2, 4, 6, 8), true, true)

    ddf1.getSchemaHandler.getColumn("month").getColumnClass should be(ColumnClass.FACTOR)
    // {'[2,4]'=1, '(4,6]'=2, '(6,8]'=3}
    ddf1.getSchemaHandler.getColumn("month").getOptionalFactor.getLevelMap.get("[2,4]") should be(1)
    //test mutable binning
    ddf.setMutable(true)
    ddf.binning("distance", "EQUALINTERVAL", 3, null, true, true)
    ddf.getSchemaHandler.getColumn("distance").getColumnClass should be(ColumnClass.FACTOR)
    ddf.getSchemaHandler.getColumn("distance").getOptionalFactor.getLevelMap.size should be(3)
    val table = ddf.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Array[Object]]): _*).asInstanceOf[DataSet[Array[Object]]]
    val collection = table.collect()
    val list = seqAsJavaListConverter(collection)
    list.asJava.get(2).mkString(",").contains("'[162,869]'") should be(true)
  }
}
