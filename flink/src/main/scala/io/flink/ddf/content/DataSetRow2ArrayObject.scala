package io.flink.ddf.content

import io.ddf.DDF
import io.ddf.content.{ConvertFunction, Representation}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.typeinfo.RowTypeInfo

import scala.collection.JavaConversions._

class DataSetRow2ArrayObject(@transient ddf: DDF) extends ConvertFunction(ddf) {
  override def apply(rep: Representation): Representation = {
    rep.getValue match {
      case dataSet: DataSet[_] =>
        dataSet.getType() match {
          case x: RowTypeInfo =>
            val rowDataSet = dataSet.asInstanceOf[DataSet[Row]]
            val columnSize = ddf.getColumnNames.length - 1
            val arrDataSet: DataSet[Array[Object]] = rowDataSet.map {
              r =>
                val objArr: Array[Object] = (0 to columnSize).map { index =>
                  r.productElement(index).asInstanceOf[Object]
                }.toArray
                objArr
            }
            new Representation(arrDataSet, RepresentationHandler.DATASET_ARR_OBJECT.getTypeSpecsString)
        }
    }
  }
}
