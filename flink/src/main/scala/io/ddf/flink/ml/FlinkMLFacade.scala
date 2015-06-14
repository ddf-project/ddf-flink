package io.ddf.flink.ml

import java.{lang, util}

import io.ddf.DDF
import io.ddf.facades.MLFacade
import io.ddf.ml.{IModel, ISupportML}
import org.apache.flink.ml.common.ParameterMap

class FlinkMLFacade(ddf: DDF, mlSupporter: ISupportML) extends MLFacade(ddf, mlSupporter) {

  private var iddf = ddf

  override def train(trainMethodName: String, args: AnyRef*): IModel = {
    mlSupporter.train(trainMethodName, args: _*)
  }

  override def applyModel(model: IModel): DDF = {
    mlSupporter.applyModel(model)
  }

  override def applyModel(model: IModel, hasLabels: Boolean): DDF = {
    mlSupporter.applyModel(model, hasLabels)
  }

  override def applyModel(model: IModel, hasLabels: Boolean, includeFeatures: Boolean): DDF = {
    mlSupporter.applyModel(model, hasLabels, includeFeatures)
  }

  override def CVRandom(k: Int, trainingSize: Double, seed: lang.Long): util.List[util.List[DDF]] = {
    throw new UnsupportedOperationException("This method is not implemented on Flink DDF")
  }

  override def CVKFold(k: Int, seed: lang.Long): util.List[util.List[DDF]] = {
    throw new UnsupportedOperationException("This method is not implemented on Flink DDF")
  }

  override def getConfusionMatrix(model: IModel, threshold: Double): Array[Array[Long]] = {
    throw new UnsupportedOperationException("This method is not implemented on Flink DDF")
  }

  override def getDDF: DDF = iddf


  override def setDDF(theDDF: DDF): Unit = {
    iddf = theDDF
  }

  def multipleLinearRegression(): DDF = {
    val model = this.train("multipleLinearRegression")
    this.applyModel(model)
  }

  def svm(blocks: Option[Int] = None,
          iterations: Option[Int] = None,
          localIterations: Option[Int] = None,
          regularization: Option[Double] = None,
          stepsize: Option[Double] = None,
          seed: Option[Long] = None): IModel = {

    import org.apache.flink.ml.classification.SVM

    val paramMap = {
      val pmap = new ParameterMap()
      blocks.map(b => pmap.add(SVM.Blocks, b))

      iterations.map(i => pmap.add(SVM.Iterations, i))
      localIterations.map(li => pmap.add(SVM.LocalIterations, li))
      regularization.map(r => pmap.add(SVM.Regularization, r))
      stepsize.map(ss => pmap.add(SVM.Stepsize, ss))
      seed.map(s => pmap.add(SVM.Seed, s))

      pmap
    }

    this.train("svm", paramMap)
  }


}
