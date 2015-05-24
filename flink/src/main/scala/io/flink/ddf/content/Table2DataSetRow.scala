package io.flink.ddf.content

import io.ddf.DDF
import io.ddf.content.Schema.Column
import io.ddf.content.{ConvertFunction, Representation}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.{Row, Table}

import scala.collection.JavaConversions._

class Table2DataSetRow(@transient ddf: DDF) extends ConvertFunction(ddf) {
  override def apply(rep: Representation): Representation = {
    rep.getValue match {
      case x:Table=>
        val columns: List[Column] = ddf.getSchema.getColumns.toList
        val rowTypeInfo = Column2RowTypeInfo.getRowTypeInfo(columns)
        val dataSet:DataSet[Row] = x.toSet(rowTypeInfo)
        new Representation(dataSet, RepresentationHandler.DATASET_ROW.getTypeSpecsString)
    }
  }
}
