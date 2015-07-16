package io.ddf.flink.etl

import io.ddf.DDF
import io.ddf.analytics.Summary
import io.ddf.content.Schema.ColumnType
import io.ddf.flink.BaseSpec
import scala.collection.JavaConversions._

class TransformationHandlerSpec extends BaseSpec {

  val relevantData: DDF = ddf.VIEWS.project("V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8")

  it should "transform native Rserve" in {
    val resultDDF = relevantData.Transform.transformNativeRserve("newcol = V5 / V7")
    resultDDF should not be null
    resultDDF.getColumnName(29) should be("newcol")
  }

  it should "transform native map reduce" in {

    val mapFuncDef: String = "function(part) { keyval(key=part$V1, val=part$V4) }"
    val reduceFuncDef: String = "function(key, vv) { keyval.row(key=key, val=sum(vv)) }"

    val subset = relevantData.VIEWS.project(List("V1","V4"))

    val newDDF: DDF = subset.Transform.transformMapReduceNative(mapFuncDef, reduceFuncDef)
    newDDF should not be null
    newDDF.getColumnName(0) should be("key")
    newDDF.getColumnName(1) should be("val")

    newDDF.getColumn("key").getType should be(ColumnType.STRING)
    newDDF.getColumn("val").getType should be(ColumnType.INT)
  }

  it should "transform scale min max" in {

    val newddf0: DDF = relevantData.Transform.transformScaleMinMax

    val summaryArr: Array[Summary] = newddf0.getSummary
    println("result summary is"+summaryArr(0))
    summaryArr(0).min should be < (1.0)
    summaryArr(0).max should be(1.0)
  }

  it should "transform scale standard" in {
    val newDDF: DDF = relevantData.Transform.transformScaleStandard()
    newDDF.getNumRows() should be (31)
    newDDF.getSummary.length should be (8)
  }

}
