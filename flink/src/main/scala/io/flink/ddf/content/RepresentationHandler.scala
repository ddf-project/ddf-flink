package io.flink.ddf.content

import io.ddf.DDF
import io.ddf.content.{Representation, RepresentationHandler => RH}
import io.flink.ddf.content.RepresentationHandler._
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.{Table, Row}
import org.rosuda.REngine.REXP

class RepresentationHandler(ddf: DDF) extends RH(ddf) {
  override def getDefaultDataType: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Array[Object]])

  //In the current implementation of ArrayObject2Row, NA,null and invalid field values are translated to default values.
  //Use DataSet[Array[Object]] if this information is required.
  this.addConvertFunction(DATASET_ARR_OBJECT, DATASET_ROW, new ArrayObject2Row(this.ddf))
  this.addConvertFunction(DATASET_ROW, TABLE, new DataSetRow2Table(this.ddf))
  this.addConvertFunction(DATASET_ROW, DATASET_ARR_OBJECT, new Row2ArrayObject(this.ddf))
  this.addConvertFunction(TABLE, DATASET_ROW, new Table2DataSetRow(this.ddf))
  this.addConvertFunction(DATASET_ARR_OBJECT, DATASET_REXP, new ArrayObject2REXP(this.ddf))
}

object RepresentationHandler {
  /**
   * Supported Representations
   */
  val DATASET_ARR_OBJECT = new Representation(classOf[DataSet[_]], classOf[Array[Object]])
  val DATASET_ROW = new Representation(classOf[DataSet[_]], classOf[Row])
  val TABLE = new Representation(classOf[Table])
  val DATASET_REXP = new Representation(classOf[DataSet[_]], classOf[REXP])

}
