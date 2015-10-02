package io.ddf.flink.ml

import io.ddf.ml.Model
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.pipeline.Predictor


class FlinkModel(rawModel: Predictor[_], rh: ReflectHelper) extends Model(rawModel) {

  import scala.reflect.runtime.{universe => ru}
  import ru._

  private val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
  val modelMirror = runtimeMirror.reflect(rawModel)
  val predictMethodSymbol = modelMirror.symbol.typeSignature.member(newTermName("predict")).asMethod

  val predictMethod = modelMirror.reflectMethod(predictMethodSymbol)

  def getTestingType: Seq[Class[_]] = rh.getDataSetType(DatasetTypes.test)

  def getResultType: Seq[Class[_]] = rh.getDataSetType(DatasetTypes.result)

  def pmap: ParameterMap = ParameterMap.Empty

  def predict(dataset: DataSet[_]): DataSet[_] = {
    predictMethod(dataset, pmap, rh.predictOperator).asInstanceOf[DataSet[_]]
  }
}

