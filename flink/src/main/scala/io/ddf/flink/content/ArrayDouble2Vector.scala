package io.ddf.flink.content

import io.ddf.DDF
import io.ddf.content.{ConvertFunction, Representation}
import io.ddf.exception.DDFException
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.{Vector => FVector, DenseVector}
import org.apache.flink.api.scala._

class ArrayDouble2Vector (@transient ddf: DDF) extends ConvertFunction(ddf) {
  override def apply(representation: Representation): Representation = {
    val numCols = ddf.getNumColumns
    representation.getValue match {
      case dataset: DataSet[Array[Double]] => {
        val vector: DataSet[FVector] = dataset.map {
          row => DenseVector(row)
        }
        new Representation(vector, RepresentationHandler.DATASET_VECTOR.getTypeSpecsString)
      }
      case _ => throw new DDFException("Error getting RDD[Vector]")
    }
  }

}
