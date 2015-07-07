package io.ddf.flink.ml

import io.ddf.flink.BaseSpec
import org.apache.flink.api.scala._
import org.apache.flink.api.table.{Row, Table}

class MLSupporterSpec extends BaseSpec {

  it should "run svm prediction" in {
    val trainData = loadIrisTrain()
    val testData = loadIrisTest()

    val imodel = trainData.ML.asInstanceOf[FlinkMLFacade].svm()
    val result = testData.ML.applyModel(imodel)
    val rows: Long = result.getNumRows
    assert(rows > 0)
    //val ds = result.getRepresentationHandler.get(classOf[DataSet[_]], classOf[Array[Object]]).asInstanceOf[DataSet[Array[Object]]].collect()
  }

  it should "run mlr" in {
    val trainData = loadRegressionTrain()
    val testData = loadRegressionTest()

    val imodel = trainData.ML.asInstanceOf[FlinkMLFacade].mlr()
    val result = testData.ML.applyModel(imodel)

    val rows: Long = result.getNumRows
    assert(rows > 0)
  }
}
