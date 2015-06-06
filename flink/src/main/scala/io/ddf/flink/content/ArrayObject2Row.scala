package io.ddf.flink.content

import io.ddf.DDF
import io.ddf.content.Schema.Column
import io.ddf.content.{ConvertFunction, Representation}
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.Row

import scala.collection.JavaConversions._

class ArrayObject2Row(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(rep: Representation): Representation = {
    val repValue: AnyRef = rep.getValue
    repValue match {
      case dataSet: DataSet[_] =>
        dataSet.getType() match {
          case x: ObjectArrayTypeInfo[_, _] =>
            val columns: List[Column] = ddf.getSchema.getColumns.toList
            val rowDataSet: DataSet[Row] = RepresentationHandler.getRowDataSet(dataSet,columns)
            new Representation(rowDataSet, RepresentationHandler.DATASET_ROW.getTypeSpecsString)
        }

    }
  }

}
