package io.ddf.flink.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction}
import io.ddf.exception.DDFException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.api.scala._

class ArrayDouble2LabeledVector(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    val numCols = ddf.getNumColumns
    representation.getValue match {
      case dataset: DataSet[Array[Double]] => {
        val datasetLabeledVector: DataSet[LabeledVector] = dataset.map {
          row => {
            val label = row.head
            val features = row.tail
            new LabeledVector(label, DenseVector(features))
          }
        }
        new Representation(datasetLabeledVector, RepresentationHandler.DATASET_LABELED_VECTOR.getTypeSpecsString)
      }
      case _ => throw new DDFException("Error getting RDD[LabeledVector]")
    }
  }
}
