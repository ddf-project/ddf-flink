package io.ddf.flink.content

import io.ddf.DDF
import io.ddf.content.{ConvertFunction, Representation}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.{Row, Table}
import org.apache.flink.api.scala.table._

class DataSetRow2Table(@transient ddf: DDF) extends ConvertFunction(ddf) {
  override def apply(rep: Representation): Representation = {
    rep.getValue match {
      case dataSet: DataSet[Row @unchecked] =>
        val rowDataSet: Table = dataSet
        new Representation(rowDataSet, RepresentationHandler.TABLE.getTypeSpecsString)
    }
  }
}
