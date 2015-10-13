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

  // scalastyle:off method.name
  override def CVRandom(k: Int, trainingSize: Double, seed: lang.Long): util.List[util.List[DDF]] = {
    this.getMLSupporter.CVRandom(k, trainingSize, seed)
  }

  override def CVKFold(k: Int, seed: lang.Long): util.List[util.List[DDF]] = {
    this.getMLSupporter.CVKFold(k, seed)
  }

  // scalastyle:on method.name

  override def getConfusionMatrix(model: IModel, threshold: Double): Array[Array[Long]] = {
    this.getMLSupporter.getConfusionMatrix(model, threshold)
  }

  override def getDDF: DDF = iddf


  override def setDDF(theDDF: DDF): Unit = {
    iddf = theDDF
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

  def mlr(iterations: Option[Int] = None,
          stepsize: Option[Double] = None,
          convergenceThreshold: Option[Double] = None): IModel = {

    import org.apache.flink.ml.regression.MultipleLinearRegression

    val paramMap = {
      val pmap = new ParameterMap()

      iterations.map(i => pmap.add(MultipleLinearRegression.Iterations, i))
      stepsize.map(ss => pmap.add(MultipleLinearRegression.Stepsize, ss))
      convergenceThreshold.map(s => pmap.add(MultipleLinearRegression.ConvergenceThreshold, s))

      pmap
    }

    this.train("mlr", paramMap)
  }

  def als(numFactors: Option[Int] = None,
          lambda: Option[Double] = None,
          iterations: Option[Int] = None,
          blocks: Option[Int] = None,
          seed: Option[Long] = None,
          temporaryPath: Option[String] = None): IModel = {

    import org.apache.flink.ml.recommendation.ALS

    val paramMap = {
      val pmap = new ParameterMap()
      blocks.map(b => pmap.add(ALS.Blocks, b))

      numFactors.map(i => pmap.add(ALS.NumFactors, i))
      lambda.map(i => pmap.add(ALS.Lambda, i))
      iterations.map(i => pmap.add(ALS.Iterations, i))
      seed.map(s => pmap.add(ALS.Seed, s))
      temporaryPath.map(p => pmap.add(ALS.TemporaryPath, p))

      pmap
    }

    this.train("als", paramMap)
  }

}
