package io.flink.ddf.content

import io.ddf.DDF
import io.ddf.content.{ConvertFunction, Representation}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.{Table, Row}
import org.apache.flink.api.table.typeinfo.RowTypeInfo

import scala.collection.JavaConversions._

class DataSetRow2Table(@transient ddf: DDF) extends ConvertFunction(ddf) {
  override def apply(rep: Representation): Representation = {
    rep.getValue match {
      case dataSet: DataSet[_] =>
        dataSet.getType() match {
          case x: RowTypeInfo =>
            val columnNames = ddf.getColumnNames.mkString(",")
            val rowDataSet:Table = dataSet.asInstanceOf[DataSet[Row]].as(columnNames)
            new Representation(rowDataSet, RepresentationHandler.TABLE.getTypeSpecsString)
        }
    }
  }
}
