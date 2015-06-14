package io.ddf.flink.content

import java.util

import io.ddf.DDF
import io.ddf.content.Schema.{Column, ColumnType}
import io.ddf.content.{Representation, ConvertFunction}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

class ArrayDouble2ArrayObject(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    import scala.collection.JavaConversions._
    val columns: util.List[Column] = ddf.getSchemaHandler.getColumns
    val mappers = columns.map {
      column => this.getDouble2ObjectMapper(column.getType)
    }
    val rddArrObj = representation.getValue match {
      case dataset: DataSet[Array[Double]] => dataset.map {
        row => {
          var i = 0
          val arrObj = new Array[Object](row.size)
          while (i < row.size) {
            arrObj(i) = mappers(i)(row(i))
            i += 1
          }
          arrObj
        }
      }
    }
    new Representation(rddArrObj, RepresentationHandler.DATASET_ARR_OBJECT.getTypeSpecsString)
  }

  private def getDouble2ObjectMapper(colType: ColumnType): Double => Object = {
    colType match {
      case ColumnType.DOUBLE => {
        case double => double.asInstanceOf[Object]
      }
      case ColumnType.INT => {
        case double => double.toInt.asInstanceOf[Object]
      }
    }
  }
}
