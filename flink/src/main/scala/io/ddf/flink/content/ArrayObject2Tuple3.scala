package io.ddf.flink.content

import io.ddf.DDF
import io.ddf.content.{ConvertFunction, Representation}
import io.ddf.exception.DDFException
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

class ArrayObject2Tuple3(@transient ddf: DDF) extends ConvertFunction(ddf) {
  override def apply(representation: Representation): Representation = {
    representation.getValue match {
      case dataset: DataSet[Array[Object]] => {
        val datasetTuple: DataSet[Tuple3[Int, Int, Double]] = dataset.map {
          row => {
            val field1 = row(0).toString.toInt
            val field2 = row(1).toString.toInt
            val field3 = row(2).toString.toDouble
            (field1, field2, field3)
          }
        }
        new Representation(datasetTuple, RepresentationHandler.DATASET_TUPLE3.getTypeSpecsString)
      }
      case _ => throw new DDFException("Error getting DataSet[(Int,Int)]")
    }
  }
}
