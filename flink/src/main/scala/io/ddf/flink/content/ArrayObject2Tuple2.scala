package io.ddf.flink.content

import io.ddf.DDF
import io.ddf.content.{ConvertFunction, Representation}
import io.ddf.exception.DDFException
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

class ArrayObject2Tuple2(@transient ddf: DDF) extends ConvertFunction(ddf) {
  override def apply(representation: Representation): Representation = {
    representation.getValue match {
      case dataset: DataSet[Array[Object]] => {
        val datasetTuple: DataSet[Tuple2[Int,Int]] = dataset.map {
          row => {
            val field1 = row(0).toString.toInt
            val field2 = row(1).toString.toInt
            (field1, field2)
          }
        }
        new Representation(datasetTuple, RepresentationHandler.DATASET_TUPLE2.getTypeSpecsString)
      }
      case _ => throw new DDFException("Error getting DataSet[(Int,Int)]")
    }
  }
}
