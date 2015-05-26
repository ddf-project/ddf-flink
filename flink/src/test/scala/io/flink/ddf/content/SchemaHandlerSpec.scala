package io.flink.ddf.content

import java.util

import io.ddf.content.Schema.Column
import io.flink.ddf.BaseSpec
import scala.collection.JavaConversions._

class SchemaHandlerSpec extends BaseSpec {

  it should "get schema" in {
    ddf.getSchema should not be null
  }

  it should "get columns" in {
    val columns: util.List[Column] = ddf.getSchema.getColumns
    columns should not be null
    columns.length should be(29)
    columns.head.getName should be("V1")
  }

  it should "get columns for sql2ddf create table" in {
    val ddf = loadAirlineDDF()
    val columns = ddf.getSchema.getColumns
    columns should not be null
    columns.length should be(29)
    columns.head.getName should be("Year")
  }

}
