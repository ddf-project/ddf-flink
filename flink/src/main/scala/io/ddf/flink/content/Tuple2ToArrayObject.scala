package io.ddf.flink.content

import io.ddf.DDF
import io.ddf.content.{ConvertFunction, Representation}
import io.ddf.exception.DDFException
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

class Tuple2ToArrayObject(@transient ddf: DDF) extends ConvertFunction(ddf) {
  override def apply(rep: Representation): Representation = {
    rep.getValue match {
      case dataset: DataSet[Tuple2[Int, Int]] =>
        val datasetArrObject: DataSet[Array[Object]] = dataset.map {
          entry =>
            val result:Seq[Object] = Seq(entry._1.asInstanceOf[Object], entry._2.asInstanceOf[Object])
            result.toArray
        }
        new Representation(datasetArrObject, RepresentationHandler.DATASET_ARR_OBJECT.getTypeSpecsString)
      case _ =>
        throw new DDFException("Error getting DataSet[Array[Object]]")
    }
  }
}
