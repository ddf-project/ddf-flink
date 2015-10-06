package io.ddf.flink.content

import io.ddf.DDF
import io.ddf.content.{Representation, ConvertFunction}
import io.ddf.exception.DDFException
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

class ArrayObject2ArrayDouble(@transient ddf: DDF) extends ConvertFunction(ddf) with ObjectToDoubleMapper {
  implicit val aoti = createTypeInformation[Array[Object]]

  override def apply(representation: Representation): Representation = {
    representation.getValue match {
      case dataset: DataSet[Array[Object]] => {
        val mappers = getMapper(ddf.getSchemaHandler.getColumns)
        val datasetArrDouble: DataSet[Array[Double]] = dataset.map {
          array => {
            val arr = new Array[Double](array.size)
            var i = 0
            var isNULL = false
            while ((i < array.size) && !isNULL) {
              mappers(i)(array(i)) match {
                case Some(number) => arr(i) = number
                case None => isNULL = true
              }
              i += 1
            }
            if (isNULL) null else arr
          }
        }.filter(row => row != null)
        new Representation(datasetArrDouble, RepresentationHandler.DATASET_ARR_DOUBLE.getTypeSpecsString)
      }
      case _ => throw new DDFException("Error getting RDD[Array[Double]]")
    }
  }
}


import io.ddf.content.Schema.{ColumnType, Column}
import scala.collection.JavaConversions._
import io.ddf.exception.DDFException
import io.ddf.flink.utils.Misc.isNull
/**
 *
 */
trait ObjectToDoubleMapper {

  def getMapper(columns: java.util.List[Column]): Array[Object => Option[Double]] = {
    columns.map(
      column => getDoubleMapper(column.getType)
    ).toArray
  }

  private def getDoubleMapper(colType: ColumnType): Object => Option[Double] = {
    colType match {
      case ColumnType.DOUBLE => {
        case obj => if (!isNull(obj)) Some(obj.asInstanceOf[Double]) else None
      }

      case ColumnType.INT => {
        case obj => if (!isNull(obj)) Some(obj.asInstanceOf[Int].toDouble) else None
      }

      case ColumnType.STRING => {
        case _ => throw new DDFException("Cannot convert string to double")
      }

      case e => throw new DDFException("Cannot convert to double")
    }
  }
}
