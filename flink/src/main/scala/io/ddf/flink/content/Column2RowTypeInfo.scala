package io.ddf.flink.content

import java.util.Date

import io.ddf.content.Schema.{Column, ColumnType}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.{ObjectArrayTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.expressions.{Expression, ResolvedFieldReference}
import org.apache.flink.api.table.typeinfo.RowTypeInfo
import org.apache.flink.api.scala._

object Column2RowTypeInfo extends Serializable {


  def getRowTypeInfo(columns: Seq[Column]): RowTypeInfo = {
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

  def getColumns(rowTypeInfo: CaseClassTypeInfo[Row]): Seq[Column] = {
    rowTypeInfo.fieldNames.map {
      col =>

        val rowIndex = rowTypeInfo.getFieldIndex(col)
        val fieldType= rowTypeInfo.getTypeAt(rowIndex).getTypeClass.getName
        val colType = fieldType match {
          case "java.lang.String" => ColumnType.STRING
          case "java.lang.Integer" => ColumnType.INT
          case "java.lang.Long" => ColumnType.LONG
          case "java.lang.Float"=> ColumnType.FLOAT
          case "java.lang.Double"=> ColumnType.DOUBLE
          case "java.util.Date"=> ColumnType.TIMESTAMP
          case "java.lang.Boolean"=> ColumnType.LOGICAL
        }
        new Column(col, colType)
    }
  }
}

