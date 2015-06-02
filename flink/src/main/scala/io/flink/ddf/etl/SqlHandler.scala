package io.flink.ddf.etl

import java.util
import java.util.Collections

import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.content.Schema.{Column, DataFormat}
import io.ddf.etl.ASqlHandler
import io.flink.ddf.FlinkDDFManager
import io.flink.ddf.content.Column2RowTypeInfo
import io.flink.ddf.content.SqlSupport._
import io.flink.ddf.utils.{Joins, Sorts}
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.api.table.typeinfo.{RenamingProxyTypeInfo, RowTypeInfo}
import org.apache.flink.api.table.{Row, Table}

import scala.collection.JavaConversions._


class SqlHandler(theDDF: DDF) extends ASqlHandler(theDDF) {

  val parser = new TableDdlParser

  def parse(input: String): Function = parser.parse(input)

  override def sql2ddf(command: String): DDF = {
    val fn = parse(command)
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
    val ddf = this.getManager.getDDFByName(l.tableName)
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
    s.validate
    val ddf = this.getManager.getDDFByName(s.relations.head.getTableName)
    val typeSpecs: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Row])
    val table: DataSet[Row] = ddf.getRepresentationHandler.get(typeSpecs: _*).asInstanceOf[DataSet[Row]]
    val where = s.where.orNull
    val group = s.group.orNull
    var joined = table
    var joinedSchemaCols = ddf.getSchema.getColumns

    val joins = s.relations.filter(i => i.isInstanceOf[JoinRelation])
    if (s.project.isStar) {
      joined = joined.select(ddf.getSchema.getColumnNames.mkString(","))
    } else {
      joined = joined.select(s.project.asInstanceOf[ExprProjection].expressions: _*)
    }

    var cols: List[Schema.Column] = null
    joins.map { j =>
      val join = j.asInstanceOf[JoinRelation]
      val ddf2 = this.getManager.getDDFByName(join.withTableName)
      val table2: DataSet[Row] = ddf2.getRepresentationHandler.get(typeSpecs: _*).asInstanceOf[DataSet[Row]]
      val (newCols, newDS) = Joins.joinDataSets(join.joinType, null,
        join.joinCondition.left, join.joinCondition.right,
        table, table2,
        ddf.getSchema, ddf2.getSchema)
      cols = newCols.toList
      joined = newDS
    }
    if (where != null) joined = joined.where(where.expression)
    if (group != null) joined = joined.groupBy(group.expression: _*)

    if (cols == null) {
      def typeInfo: RowTypeInfo = joined.getType() match {
        case r: RenamingProxyTypeInfo[Row] => r.getUnderlyingType.asInstanceOf[RowTypeInfo]
        case t: RowTypeInfo => t
      }
      cols = Column2RowTypeInfo.getColumns(typeInfo).toList
    }
    val tableName = ddf.getSchemaHandler.newTableName
    val schema = new Schema(tableName, cols)
    joined = s.order.map { ord =>
      val orderByFields = ord.columns.map(_._1)
      val order: Array[Boolean] = ord.columns.map(_._2).toArray
      Sorts.sort(joined, schema, orderByFields, order)
    }.getOrElse(joined)

    if (s.limit > 0) joined = joined.first(s.limit)

    val newDDF = this.getManager.newDDF(joined, dsrTypeSpecs, null, tableName, schema)
    newDDF
  }

  override def sql2ddf(command: String, schema: Schema): DDF = sql2ddf(command)

  override def sql2ddf(command: String, dataFormat: DataFormat): DDF = sql2ddf(command)

  override def sql2ddf(command: String, schema: Schema, dataSource: String): DDF = sql2ddf(command)

  override def sql2ddf(command: String, schema: Schema, dataFormat: DataFormat): DDF = sql2ddf(command)

  override def sql2ddf(command: String, schema: Schema, dataSource: String, dataFormat: DataFormat): DDF = sql2ddf(command)

  override def sql2txt(command: String): util.List[String] = {
    val fn = parse(command)
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
