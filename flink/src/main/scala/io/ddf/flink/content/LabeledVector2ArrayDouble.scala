package io.ddf.flink.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction}
import io.ddf.exception.DDFException
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.api.scala._


class LabeledVector2ArrayDouble(@transient ddf: DDF) extends ConvertFunction(ddf) {
  override def apply(rep: Representation): Representation = {
    rep.getValue match {
      case dataset: DataSet[LabeledVector] =>
        val datasetArrDouble: DataSet[Array[Double]] = dataset.map {
          lvector =>
            Array[Double](lvector.label) ++ lvector.vector.map(_._2).toArray[Double]
        }
        new Representation(datasetArrDouble, RepresentationHandler.DATASET_ARR_DOUBLE.getTypeSpecsString)
      case _ =>
        throw new DDFException("Error getting RDD[Array[Double]]")
    }
  }
}