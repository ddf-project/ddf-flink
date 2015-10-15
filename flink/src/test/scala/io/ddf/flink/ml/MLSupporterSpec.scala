package io.ddf.flink.ml

import io.ddf.flink.BaseSpec
import org.apache.flink.ml.clustering.KMeans

class MLSupporterSpec extends BaseSpec {

  it should "run svm prediction" in {
    val trainData = loadIrisTrain()
    val testData = loadIrisTest()

    val imodel = trainData.ML.asInstanceOf[FlinkMLFacade].svm()
    val result = testData.ML.applyModel(imodel)
    val rows: Long = result.getNumRows
    assert(rows > 0)
  }

  it should "run mlr" in {
    val trainData = loadRegressionTrain()
    val testData = loadRegressionTest()

    val imodel = trainData.ML.asInstanceOf[FlinkMLFacade].mlr()
    val result = testData.ML.applyModel(imodel)

    val rows: Long = result.getNumRows
    assert(rows > 0)
  }

  it should "run als" in {
    val trainData = loadRatingsTrain()
    val testData = loadRatingsTest()

    val imodel = trainData.ML.asInstanceOf[FlinkMLFacade].als()
    val result = testData.ML.applyModel(imodel)

    val rows: Long = result.getNumRows
    assert(rows > 0)
  }

  it should "run kmeans" in {
    val airlineDDF = loadAirlineDDF()
    val trainData = flinkDDFManager.sql2ddf("select DepTime, ArrTime, Distance, DepDelay, ArrDelay from airline")

    val imodel = trainData.ML.asInstanceOf[FlinkMLFacade].kMeans(Option(5), Option(10))
    val kmeans: KMeans = imodel.getRawModel.asInstanceOf[KMeans]
    val points = kmeans.centroids.get.collect().flatten
    points should have size(5)
  }
}
