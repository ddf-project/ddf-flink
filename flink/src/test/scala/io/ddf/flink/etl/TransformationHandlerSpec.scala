package io.ddf.flink.etl

import io.ddf.DDF
import io.ddf.content.Schema.ColumnType
import io.ddf.flink.BaseSpec
import scala.collection.JavaConversions._

class TransformationHandlerSpec extends BaseSpec {

  it should "transform native Rserve" in {
    val resultDDF = ddf.Transform.transformNativeRserve("newcol = V5 / V7")
    resultDDF should not be null
    resultDDF.getColumnName(29) should be("newcol")
  }

  it should "transform native map reduce" in {

    val mapFuncDef: String = "function(part) { keyval(key=part$V1, val=part$V4) }"
    val reduceFuncDef: String = "function(key, vv) { keyval.row(key=key, val=sum(vv)) }"

    val subset = ddf.VIEWS.project(List("V1","V4"))

    val newddf: DDF = subset.Transform.transformMapReduceNative(mapFuncDef, reduceFuncDef)
    newddf should not be null
    newddf.getColumnName(0) should be("key")
    newddf.getColumnName(1) should be("val")

    newddf.getColumn("key").getType should be(ColumnType.STRING)
    newddf.getColumn("val").getType should be(ColumnType.INT)
  }

}
