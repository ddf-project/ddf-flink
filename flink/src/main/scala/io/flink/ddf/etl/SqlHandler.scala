package io.flink.ddf.etl

import java.util
import java.util.Collections

import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.content.Schema.{Column, DataFormat}
import io.ddf.etl.ASqlHandler
import io.flink.ddf.FlinkDDFManager
import io.flink.ddf.content.SqlSupport._
import io.flink.ddf.content.{Column2RowTypeInfo, SchemaHandler}
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.api.table.typeinfo.{RenamingProxyTypeInfo, RowTypeInfo}
import org.apache.flink.api.table.{Row, Table}

import scala.collection.JavaConversions._


class SqlHandler(theDDF: DDF) extends ASqlHandler(theDDF) {
  override def sql2ddf(command: String): DDF = {
    val schemaHandler: SchemaHandler = this.getDDF.getSchemaHandler.asInstanceOf[SchemaHandler]
    val fn = schemaHandler.parse(command)
    fn match {
      case c: Create =>
        val ddf = create2ddf(c)
        this.getManager.addDDF(ddf)
        ddf

      case l: Load =>
        val ddf = load2ddf(l)
        ddf

      case s: Select =>
        val newDDF = select2ddf(s)
        this.getManager.addDDF(newDDF)
        newDDF
    }
  }

  protected def load2ddf(l: Load) = {
    val ddf = this.getManager.getDDF(l.tableName)
    val env = this.getManager.asInstanceOf[FlinkDDFManager].getExecutionEnvironment
    val dataSet = env
      .readTextFile(l.url)
      .map(_.split(",").map(_.asInstanceOf[Object]))

    ddf.getRepresentationHandler.set(dataSet, dsoTypeSpecs: _*)
    ddf
  }

  protected def create2ddf(c: Create) = {
    val cols: util.List[Column] = seqAsJavaList(c.columns.map(i => new Column(i._1, i._2)).toSeq)
    val schema: Schema = new Schema(c.tableName, cols.toList)
    val ddf = this.getManager.newDDF(null, Array(classOf[String]), null, c.tableName, schema)
    ddf
  }

  protected def select2ddf(s: Select) = {
    val ddf = this.getManager.getDDF(s.from.str.head)
    val typeSpecs: Array[Class[_]] = Array(classOf[Table])
    val table: Table = ddf.getRepresentationHandler.get(typeSpecs: _*).asInstanceOf[Table]
    val join = s.join.orNull
    val where = s.where.orNull
    val group = s.group.orNull
    var joined = table
    if (s.from.str.size > 1 && join != null) {
      val ddf2 = this.getManager.getDDF(s.from.str.tail.head)
      val table2: Table = ddf2.getRepresentationHandler.get(typeSpecs: _*).asInstanceOf[Table]
      joined = table.join(table2).where(join.expression)
    }
    joined = joined.select(s.project.expression: _*)
    if (where != null) joined = joined.where(where.expression)
    if (group != null) joined = joined.groupBy(group.expression: _*)
    val dataSet = joined.toSet[Row]
    val typeInfo: RowTypeInfo = dataSet.getType() match {
      case r: RenamingProxyTypeInfo[Row] => r.getUnderlyingType.asInstanceOf[RowTypeInfo]
      case t: RowTypeInfo => t
    }
    val tableName = ddf.getSchemaHandler.newTableName
    val schema = new Schema(tableName, Column2RowTypeInfo.getColumns(typeInfo))
    val newDDF = this.getManager.newDDF(dataSet, dsrTypeSpecs, null, tableName, schema)
    newDDF
  }

  override def sql2ddf(command: String, schema: Schema): DDF = sql2ddf(command)

  override def sql2ddf(command: String, dataFormat: DataFormat): DDF = sql2ddf(command)

  override def sql2ddf(command: String, schema: Schema, dataSource: String): DDF = sql2ddf(command)

  override def sql2ddf(command: String, schema: Schema, dataFormat: DataFormat): DDF = sql2ddf(command)

  override def sql2ddf(command: String, schema: Schema, dataSource: String, dataFormat: DataFormat): DDF = sql2ddf(command)

  override def sql2txt(command: String): util.List[String] = {
    val schemaHandler: SchemaHandler = this.getDDF.getSchemaHandler.asInstanceOf[SchemaHandler]
    val fn = schemaHandler.parse(command)
    fn match {
      case c: Create =>
        val ddf = create2ddf(c)
        this.getManager.addDDF(ddf)
        Collections.singletonList("0")

      case l: Load =>
        val ddf = load2ddf(l)
        val table: Table = ddf.getRepresentationHandler.get(tTypeSpecs: _*).asInstanceOf[Table]
        seqAsJavaList(table.collect().map(_.toString()))

      case s: Select =>
        val ddf = select2ddf(s)

        val table: Table = ddf.getRepresentationHandler.get(tTypeSpecs: _*).asInstanceOf[Table]
        seqAsJavaList(table.collect().map(_.toString()))
    }
  }

  override def sql2txt(command: String, maxRows: Integer): util.List[String] = sql2txt(command)

  override def sql2txt(command: String, maxRows: Integer, dataSource: String): util.List[String] = sql2txt(command)

  val dsrTypeSpecs: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Row])
  val tTypeSpecs: Array[Class[_]] = Array(classOf[Table])
  val dsoTypeSpecs: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Array[Object]])
}
