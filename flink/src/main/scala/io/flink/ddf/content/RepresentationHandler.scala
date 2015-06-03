package io.flink.ddf.content

import java.text.SimpleDateFormat
import java.util.Date

import io.ddf.DDF
import io.ddf.content.Schema.{ColumnType, Column}
import io.ddf.content.{Representation, RepresentationHandler => RH}
import io.flink.ddf.content.RepresentationHandler._
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.{Table, Row}
import org.rosuda.REngine.REXP

import scala.util.{Failure, Success, Try}

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

  val DATASET_ROW_TYPE_SPECS = Array(classOf[DataSet[_]], classOf[Row])
  val TABLE_TYPE_SPECS = Array(classOf[Table])
  val DATASET_ARR_OBJ_TYPE_SPECS = Array(classOf[DataSet[_]], classOf[Array[Object]])

  private val dateFormat = new SimpleDateFormat()

  def getRowDataSet(dataSet: DataSet[_],columns: List[Column]): DataSet[Row] = {
    val idxColumns: Seq[(Column, Int)] = columns.zipWithIndex.toSeq
    implicit val rowTypeInfo = Column2RowTypeInfo.getRowTypeInfo(columns)
    val rowDataSet = dataSet.asInstanceOf[DataSet[Array[Object]]].map(r => parseRow(r, idxColumns))
    rowDataSet
  }
  //TODO check what to set as defaults for different types - setting null throws error when serializing for the map job
  private def getFieldValue(elem: Object, isNumeric: Boolean): String = {
    val mayBeString: Try[String] = Try(elem.toString.trim)
    mayBeString match {
      case Success(s) if isNumeric && s.equalsIgnoreCase("NA") => "0"
      case Success(s) => s
      case Failure(e) if isNumeric => "0"
      case Failure(e) => ""
    }
  }

  private def parseRow(rowArray: Array[Object], idxColumns: Seq[(Column, Int)]): Row = {
    val row = new Row(idxColumns.length)
    idxColumns foreach {
      case (col, idx) =>
        val colValue: String = getFieldValue(rowArray(idx), col.isNumeric)
        col.getType match {
          case ColumnType.STRING =>
            row.setField(idx, colValue)
          case ColumnType.INT =>
            row.setField(idx, Try(colValue.toInt).getOrElse(0))
          case ColumnType.LONG =>
            row.setField(idx, Try(colValue.toLong).getOrElse(0))
          case ColumnType.FLOAT =>
            row.setField(idx, Try(colValue.toFloat).getOrElse(0))
          case ColumnType.DOUBLE =>
            row.setField(idx, Try(colValue.toDouble).getOrElse(0))
          case ColumnType.BIGINT =>
            row.setField(idx, Try(colValue.toDouble).getOrElse(0))
          case ColumnType.TIMESTAMP =>
            row.setField(idx, Try(dateFormat.parse(colValue)).getOrElse(new Date(0)))
          case ColumnType.LOGICAL =>
            row.setField(idx, Try(colValue.toBoolean).getOrElse(false))
        }
    }
    row
  }


}
