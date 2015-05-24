package io.flink.ddf.content

import java.text.SimpleDateFormat
import java.util.Date

import io.ddf.DDF
import io.ddf.content.Schema.{Column, ColumnType}
import io.ddf.content.{ConvertFunction, Representation}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.expressions.{Expression, ResolvedFieldReference}
import org.apache.flink.api.table.typeinfo.RowTypeInfo

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class ArrayObject2Row(@transient ddf: DDF) extends ConvertFunction(ddf) {

  private def getRowTypeInfo(columns: Seq[Column]): RowTypeInfo = {
    val fields: Seq[Expression] = columns.map {
      col =>
        val fieldType = col.getType match {
          case ColumnType.STRING => BasicTypeInfo.STRING_TYPE_INFO
          case ColumnType.INT => BasicTypeInfo.INT_TYPE_INFO
          case ColumnType.LONG => BasicTypeInfo.LONG_TYPE_INFO
          case ColumnType.FLOAT => BasicTypeInfo.FLOAT_TYPE_INFO
          case ColumnType.DOUBLE => BasicTypeInfo.DOUBLE_TYPE_INFO
          case ColumnType.BIGINT => BasicTypeInfo.DOUBLE_TYPE_INFO
          case ColumnType.TIMESTAMP => BasicTypeInfo.DATE_TYPE_INFO
          case ColumnType.LOGICAL => BasicTypeInfo.BOOLEAN_TYPE_INFO
        }
        ResolvedFieldReference(col.getName, fieldType)
    }
    new RowTypeInfo(fields)
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

  private val dateFormat = new SimpleDateFormat()

  override def apply(rep: Representation): Representation = {
    val repValue: AnyRef = rep.getValue
    repValue match {
      case dataSet: DataSet[_] =>
        dataSet.getType() match {
          case x: ObjectArrayTypeInfo[Array[Object], _] =>
            val columns: List[Column] = ddf.getSchema.getColumns.toList
            val idxColumns: Seq[(Column, Int)] = columns.zipWithIndex.toSeq
            implicit val rowTypeInfo = getRowTypeInfo(columns)
            val rowDataSet = dataSet.asInstanceOf[DataSet[Array[Object]]].map(r => parseRow(r, idxColumns))
            new Representation(rowDataSet, RepresentationHandler.DATASET_ROW.getTypeSpecsString)
        }

    }
  }
}
