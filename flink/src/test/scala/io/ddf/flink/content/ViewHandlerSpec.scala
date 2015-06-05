package io.ddf.flink.content

import io.ddf.DDF
import io.ddf.content.ViewHandler.{OperationName, Operator}
import io.ddf.flink.BaseSpec

class ViewHandlerSpec extends BaseSpec {
  val airlineDDF = loadAirlineDDF()
  val yearNamesDDF = loadYearNamesDDF()

  it should "project after remove columns " in {
    val ddf = airlineDDF
    val columns: java.util.List[String] = new java.util.ArrayList()
    columns.add("year")
    columns.add("month")
    columns.add("deptime")

    val newddf1: DDF = ddf.VIEWS.removeColumn("year")
    val newddf2: DDF = ddf.VIEWS.removeColumns("year", "deptime")
    val newddf3: DDF = ddf.VIEWS.removeColumns(columns)
    newddf1.getNumColumns should be(28)
    newddf2.getNumColumns should be(27)
    newddf3.getNumColumns should be(26)
  }

//  it should "test subsetting with grep" in {
//    val ddf = airlineDDF
//    val columns: java.util.List[ViewHandler.Column] = new java.util.ArrayList()
//    val col: ViewHandler.Column = new ViewHandler.Column
//    col.setName("origin")
//    columns.add(col)
//
//    val grep: Operator = new Operator
//    grep.setName(OperationName.grep)
//    val operands: Array[ViewHandler.Expression] = new Array[ViewHandler.Expression](2)
//    val `val`: ViewHandler.StringVal = new ViewHandler.StringVal
//    `val`.setValue("IAD")
//    operands(0) = `val`
//    operands(1) = col
//    grep.setOperarands(operands)
//
//    val ddf2: DDF = ddf.VIEWS.subset(columns, grep)
//    ddf2.getNumRows should be(2)
//  }
}
