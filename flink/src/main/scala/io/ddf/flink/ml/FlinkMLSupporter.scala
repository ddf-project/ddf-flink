package io.ddf.flink.ml

import java.{lang, util}

import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.misc.{Config, ADDFFunctionalGroupHandler}
import io.ddf.ml.{IModel, ISupportML, MLSupporter => CoreMLSupporter, Model}
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

  override def CVRandom(k: Int, trainingSize: Double, seed: lang.Long): util.List[util.List[DDF]] = ???

  override def CVKFold(k: Int, seed: lang.Long): util.List[util.List[DDF]] = ???

  override def getConfusionMatrix(model: IModel, threshold: Double): Array[Array[Long]] = ???
}

object DatasetTypes {

  sealed trait EnumVal

  case object train extends EnumVal

  case object test extends EnumVal

  case object result extends EnumVal

}
