package io.ddf.flink.ml

import java.{lang, util}

import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.flink.FlinkDDF
import io.ddf.flink.analytics.CrossValidation
import io.ddf.misc.{Config, ADDFFunctionalGroupHandler}
import io.ddf.ml.{IModel, ISupportML, MLSupporter => CoreMLSupporter, Model}
import org.apache.flink.api.common.functions.{ReduceFunction, MapFunction}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.pipeline.{Estimator, Predictor}

import scala.reflect.api.JavaUniverse

class FlinkMLSupporter(ddf: DDF) extends ADDFFunctionalGroupHandler(ddf) with ISupportML with Serializable {

  def convertDDF(dsParamTypes: Seq[Class[_]]): AnyRef = {
    this.getDDF.getRepresentationHandler.get((classOf[DataSet[_]] +: dsParamTypes): _*)
  }

  override def train(trainMethodKey: String, args: AnyRef*): IModel = {
    val trainMethodName: String = Config.getValueWithGlobalDefault(this.getEngine, trainMethodKey)
    val rh = new ReflectHelper(trainMethodName)
    val dataset: DataSet[_] = convertDDF(rh.getDataSetType(DatasetTypes.train)).asInstanceOf[DataSet[_]]
    if (args.isEmpty) {
      rh.train(dataset, ParameterMap.Empty)
    } else {
      assert(args(0).isInstanceOf[ParameterMap], "The flink training method requires configuration as a ParameterMap")
      rh.train(dataset, args(0).asInstanceOf[ParameterMap])
    }
  }

  override def applyModel(model: IModel): DDF = applyModel(model, true)

  override def applyModel(model: IModel, hasLabels: Boolean): DDF = applyModel(model, hasLabels, true)

  override def applyModel(model: IModel, hasLabels: Boolean, includeFeatures: Boolean): DDF = {
    import scala.collection.JavaConverters._
    val fmodel: FlinkModel = model.asInstanceOf[FlinkModel]
    val testingType: Seq[Class[_]] = fmodel.getTestingType
    val resultDs = fmodel.predict(convertDDF(testingType).asInstanceOf[DataSet[_]])

    val resultType = fmodel.getResultType

    //TODO: Figure out column schema from the result type
    val outputColumns: List[Column] = ddf.getSchema.getColumns.asScala.toList :+ new Schema.Column("yPredict", "double")

    val schema = new Schema(null, outputColumns.asJava)

    this.getManager.newDDF(this.getManager, resultDs, (classOf[DataSet[_]] +: resultType).toArray, this.getManager.getNamespace, null, schema)
  }

  override def CVRandom(k: Int, trainingSize: Double, seed: lang.Long): util.List[util.List[DDF]] = {
    CrossValidation.DDFRandomSplit(this.getDDF, k, trainingSize, seed)
  }

  override def CVKFold(k: Int, seed: lang.Long): util.List[util.List[DDF]] = {
    CrossValidation.DDFKFoldSplit(this.getDDF, k, seed)
  }

  override def getConfusionMatrix(model: IModel, threshold: Double): Array[Array[Long]] = {
    val ddf: FlinkDDF = this.getDDF.asInstanceOf[FlinkDDF]
    val predictions: FlinkDDF = ddf.ML.applyModel(model, true, false).asInstanceOf[FlinkDDF]

    // Now get the underlying RDD to compute
    val yTrueYPred: DataSet[Array[Double]] = predictions.getRepresentationHandler.get(classOf[DataSet[_]], classOf[Array[Double]]).asInstanceOf[DataSet[Array[Double]]]
    val threshold1: Double = threshold
    val cm: Array[Long] = yTrueYPred.map(new MapFunction[Array[Double], Array[Long]]() {
      override def map(params: Array[Double]): Array[Long] = {
        val isPos: Byte = toByte(params(0) > threshold1)
        val predPos: Byte = toByte(params(1) > threshold1)
        val result: Array[Long] = Array[Long](0L, 0L, 0L, 0L)
        result(isPos << 1 | predPos) = 1L
        result
      }
    }).reduce(new ReduceFunction[Array[Long], Array[Long], Array[Long]]() {
      override def reduce(a: Array[Long], b: Array[Long]): Array[Long] = {
        Array[Long](a(0) + b(0), a(1) + b(1), a(2) + b(2), a(3) + b(3))
      }
    }).collect().flatten.toArray

    Array[Array[Long]](Array[Long](cm(3), cm(2)), Array[Long](cm(1), cm(0)))
  }

  private def toByte(exp: Boolean): Byte = {
    if (exp) 1 else 0
  }
}


object DatasetTypes {

  sealed trait EnumVal

  case object train extends EnumVal

  case object test extends EnumVal

  case object result extends EnumVal

}
