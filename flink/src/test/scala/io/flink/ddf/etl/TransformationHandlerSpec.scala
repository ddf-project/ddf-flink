package io.flink.ddf.etl

import io.ddf.DDF
import io.flink.ddf.BaseSpec

class TransformationHandlerSpec extends BaseSpec {

  it should "transform native Rserve" in {
    val resultDDF = ddf.Transform.transformNativeRserve("newcol = V5 / V7")
    resultDDF should not be null
    resultDDF.getColumnName(29) should be("newcol")
  }

  //TODO
  /*it should "transform native map reduce" in {

    val mapFuncDef: String = "function(part) { keyval(key=part$V1, val=part$V4) }"
    val reduceFuncDef: String = "function(key, vv) { keyval.row(key=key, val=sum(vv)) }"
    val newddf: DDF = ddf.Transform.transformMapReduceNative(mapFuncDef, reduceFuncDef)
    newddf should not be null
    newddf.getColumnName(0) should be("key")
    newddf.getColumnName(1) should be("value")
  }*/

}
