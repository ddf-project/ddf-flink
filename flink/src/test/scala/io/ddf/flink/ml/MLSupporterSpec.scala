package io.ddf.flink.ml

import io.ddf.flink.BaseSpec
import org.apache.flink.api.scala._
import org.apache.flink.api.table.{Row, Table}

class MLSupporterSpec extends BaseSpec {
  val trainData = loadIrisTrain()
  val testData = loadIrisTest()

  it should "run svm prediction" in {
    val imodel = trainData.ML.asInstanceOf[FlinkMLFacade].svm()
    val result = testData.ML.applyModel(imodel)
    val rows: Long = result.getNumRows
    assert(rows > 0)
    //val ds = result.getRepresentationHandler.get(classOf[DataSet[_]], classOf[Array[Object]]).asInstanceOf[DataSet[Array[Object]]].collect()
  }
}
