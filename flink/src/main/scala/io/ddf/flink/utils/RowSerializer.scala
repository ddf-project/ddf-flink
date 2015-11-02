package io.ddf.flink.utils

import java.util

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.table.Row
import org.apache.flink.core.memory.{DataInputView, DataOutputView}


/**
 * Custom Serializer for [[Row]] which supports null fields.
 */
class RowSerializer(fieldSerializers: Array[TypeSerializer[Any]])
  extends TypeSerializer[Row] {

  private def getFieldSerializers = fieldSerializers

  override def isImmutableType: Boolean = false

  override def getLength: Int = -1

  override def duplicate: RowSerializer = this

  override def createInstance: Row = {
    new Row(fieldSerializers.length)
  }

  override def copy(from: Row, reuse: Row): Row = {
    val len = fieldSerializers.length

    if (from.productArity != len) {
      throw new RuntimeException("Row arity of reuse and from do not match.")
    }
    var i = 0
    while (i < len) {
      val reuseField = reuse.productElement(i)
      val fromField = from.productElement(i).asInstanceOf[AnyRef]
      val copy = fieldSerializers(i).copy(fromField, reuseField)
      reuse.setField(i, copy)
      i += 1
    }
    reuse
  }

  override def copy(from: Row): Row = {
    val len = fieldSerializers.length

    if (from.productArity != len) {
      throw new RuntimeException("Row arity of reuse and from do not match.")
    }
    val result = new Row(len)
    var i = 0
    while (i < len) {
      val fromField = from.productElement(i).asInstanceOf[AnyRef]
      val copy = fieldSerializers(i).copy(fromField)
      result.setField(i, copy)
      i += 1
    }
    result
  }

  override def serialize(value: Row, target: DataOutputView) {
    val len = fieldSerializers.length
    var index = 0
    while (index < len) {
      val o: AnyRef = value.productElement(index).asInstanceOf[AnyRef]
      if (o == null) {
        target.writeBoolean(true)
      } else {
        target.writeBoolean(false)
        val serializer = fieldSerializers(index)
        serializer.serialize(value.productElement(index), target)
      }
      index += 1
    }
  }

  override def deserialize(reuse: Row, source: DataInputView): Row = {
    val len = fieldSerializers.length

    if (reuse.productArity != len) {
      throw new RuntimeException("Row arity of reuse and fields do not match.")
    }

    var index = 0
    while (index < len) {
      val isNull: Boolean = source.readBoolean
      if (isNull) {
        reuse.setField(index, null)
      } else {
        val field = reuse.productElement(index).asInstanceOf[AnyRef]
        val serializer: TypeSerializer[Any] = fieldSerializers(index)
        reuse.setField(index, serializer.deserialize(field, source))
      }
      index += 1
    }
    reuse
  }

  override def deserialize(source: DataInputView): Row = {
    val len = fieldSerializers.length

    val result = new Row(len)

    var index = 0
    while (index < len) {
      val isNull: Boolean = source.readBoolean()
      if (isNull) {
        result.setField(index, null)
      } else {
        val serializer: TypeSerializer[Any] = fieldSerializers(index)
        result.setField(index, serializer.deserialize(source))
      }
      index += 1
    }
    result
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val len = fieldSerializers.length
    var i = 0
    while (i < len) {
      val isNull = source.readBoolean()
      target.writeBoolean(isNull)
      if (!isNull) {
        fieldSerializers(i).copy(source, target)
      }
      i += 1
    }
  }

  override def equals(any: scala.Any): Boolean = {
    any match {
      case otherRS: RowSerializer =>
        val otherFieldSerializers = otherRS.getFieldSerializers.asInstanceOf[Array[AnyRef]]
        util.Arrays.deepEquals(fieldSerializers.asInstanceOf[Array[AnyRef]], otherFieldSerializers)
      case _ => false
    }
  }

  override def canEqual(o: scala.Any): Boolean = false

  override def hashCode(): Int = 1
}