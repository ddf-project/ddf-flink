package io.flink.ddf.content

import io.ddf.content.Schema.{Column, ColumnType}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
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

  def getColumns(rowTypeInfo: RowTypeInfo): Seq[Column] = {
    rowTypeInfo.fieldNames.map {
      col =>
        val fieldType = rowTypeInfo.getTypeAt(col).getTypeClass
        val colType = fieldType match {
          case BasicTypeInfo.STRING_TYPE_INFO => ColumnType.STRING
          case BasicTypeInfo.INT_TYPE_INFO => ColumnType.INT
          case BasicTypeInfo.LONG_TYPE_INFO => ColumnType.LONG
          case BasicTypeInfo.FLOAT_TYPE_INFO => ColumnType.FLOAT
          case BasicTypeInfo.DOUBLE_TYPE_INFO => ColumnType.DOUBLE
          case BasicTypeInfo.DOUBLE_TYPE_INFO => ColumnType.BIGINT
          case BasicTypeInfo.DATE_TYPE_INFO => ColumnType.TIMESTAMP
          case BasicTypeInfo.BOOLEAN_TYPE_INFO => ColumnType.LOGICAL
        }
        new Column(col, colType)
    }
  }
}

