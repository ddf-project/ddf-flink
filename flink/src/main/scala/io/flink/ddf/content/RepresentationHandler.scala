package io.flink.ddf.content

import io.ddf.DDF
import io.ddf.content.{Representation, RepresentationHandler => RH}
import io.flink.ddf.content.RepresentationHandler._
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.{Table, Row}

class RepresentationHandler(ddf: DDF) extends RH(ddf) {
  override def getDefaultDataType: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Array[Object]])

  this.addConvertFunction(DATASET_ARR_OBJECT, DATASET_ROW, new ArrayObject2Row(this.ddf))
  this.addConvertFunction(DATASET_ROW, TABLE, new DataSetRow2Table(this.ddf))
}

object RepresentationHandler {
  /**
   * Supported Representations
   */
  val DATASET_ARR_OBJECT = new Representation(classOf[DataSet[_]], classOf[Array[Object]])
  val DATASET_ROW = new Representation(classOf[DataSet[_]], classOf[Row])
  val TABLE = new Representation(classOf[Table])

}
