package io.ddf.flink.content

import io.ddf.content.IHandleRepresentations
import io.ddf.flink.BaseSpec
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.{Row, Table}

class RepresentationHandlerSpec extends BaseSpec {

  override val ddf = loadAirlineDDF()
  val handler: IHandleRepresentations = ddf.getRepresentationHandler

  it should "have default datatype as DataSet[Array[Object]]" in {
    handler.getDefaultDataType should be(Array(classOf[DataSet[_]], classOf[Array[Object]]))
  }

  it should "get underlying DataSet[Array[Object]]" in {
    val rowDataSet = handler.get(classOf[DataSet[_]], classOf[Array[Object]]).asInstanceOf[DataSet[Array[Object]]]
    val rows: Seq[Array[Object]] = rowDataSet.first(1).collect()
    rows.head(0).toString.toInt should be(2008)
  }

  it should "get DataSet[Row]" in {

    val rowDataSet = ddf.getRepresentationHandler.get(classOf[DataSet[_]], classOf[Row]).asInstanceOf[DataSet[Row]]
    val rows: Seq[Row] = rowDataSet.first(1).collect()
    rows.head.productElement(0) should be(2008)
  }

  it should "get Table" in {
    val table = ddf.getRepresentationHandler.get(classOf[Table]).asInstanceOf[Table]
    val distinctYear = table.select("Year").distinct.collect()
    //years 2008,2009 and 2010
    distinctYear.length should be(3)
  }

  it should "get DataSet[FlinkRList]" in {
    val rEXPDataSet = handler.get(classOf[DataSet[_]], classOf[FlinkRList]).asInstanceOf[DataSet[FlinkRList]]
    rEXPDataSet should not be null
    val result = rEXPDataSet.first(1).collect().head
    ddf.getSchema.getColumnNames should (contain(result.names(0)) and contain(result.names(13)))
  }
}
