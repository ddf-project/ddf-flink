package org.apache.flink.api.table

class Row(arity: Int) extends Product {

  private val fields = new Array[Any](arity)

  def productArity = fields.length

  def productElement(i: Int): Any = fields(i)

  def setField(i: Int, value: Any): Unit = fields(i) = value

  def canEqual(that: Any) = false

  override def toString = fields.mkString(",")

}
