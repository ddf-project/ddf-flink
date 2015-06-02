package io.flink.ddf.etl

import io.ddf.DDF
import io.flink.ddf.BaseSpec

class TransformationHandlerSpec extends BaseSpec {

  it should "transform native Rserve" in {
    val resultDDF = ddf.Transform.transformNativeRserve("newcol = deptime / arrtime")
    resultDDF should not be null
    resultDDF.getColumnName(8) should be("newcol")
  }

  it should "transform native map reduce" in {
    val mapFuncDef: String = "function(part) { keyval(key=part$year, val=part$month) }"
    val reduceFuncDef: String = "function(key, vv) { keyval.row(key=key, val=sum(vv)) }"
    val newddf: DDF = ddf.Transform.transformMapReduceNative(mapFuncDef, reduceFuncDef)
    newddf should not be null
    newddf.getColumnName(0) should be("key")
    newddf.getColumnName(1) should be("value")
  }

}
