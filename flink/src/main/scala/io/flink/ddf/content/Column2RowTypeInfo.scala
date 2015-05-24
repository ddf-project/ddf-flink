package io.flink.ddf.content

import io.ddf.content.Schema.{ColumnType, Column}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.table.expressions.{ResolvedFieldReference, Expression}
import org.apache.flink.api.table.typeinfo.RowTypeInfo

object Column2RowTypeInfo extends Serializable{
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
}
