package io.ddf.flink.etl

import java.util
import java.util.Collections

import io.ddf.content.Schema.Column
import io.ddf.content.{Schema, SqlResult, SqlTypedResult}
import io.ddf.datasource.{DataFormat, DataSourceDescriptor}
import io.ddf.etl.ASqlHandler
import io.ddf.flink.FlinkDDFManager
import io.ddf.flink.content.SqlSupport._
import io.ddf.flink.content.{Column2RowTypeInfo, RepresentationHandler}
import io.ddf.flink.utils.Misc._
import io.ddf.flink.utils.{Joins, Sorts, StringArrayCsvInputFormat}
import io.ddf.{DDF, DDFManager, TableNameReplacer}
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.api.table.typeinfo.{RenamingProxyTypeInfo, RowTypeInfo}
import org.apache.flink.api.table.{Row, Table}
import org.apache.flink.core.fs.Path

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


class SqlHandler(theDDF: DDF) extends ASqlHandler(theDDF) {

  val parser = new TableDdlParser

  def parse(input: String): Function = parser.parse(input)

  protected def load2ddf(l: Load) = {
    val ddf = this.getManager.getDDFByName(l.tableName)
    val env = this.getManager.asInstanceOf[FlinkDDFManager].getExecutionEnvironment
    val path = new Path(l.url)
    val csvInputFormat = new StringArrayCsvInputFormat(path, l.delimiter, emptyValue = l.emptyValue, nullValue = l.nullValue)
    val dataSet: DataSet[Array[Object]] = env.readFile(csvInputFormat, l.url).map { row =>
      row.map(_.asInstanceOf[Object])
    }
    val rowDS = RepresentationHandler.getRowDataSet(dataSet, ddf.getSchema.getColumns.asScala.toList, l.useDefaults)
    ddf.getRepresentationHandler.set(rowDS, dsrTypeSpecs: _*)
    ddf
  }

  protected def create2ddf(c: Create): DDF = {
    val cols: util.List[Column] = seqAsJavaList(c.columns.map(i => new Column(i._1, i._2)).toSeq)
    val schema: Schema = new Schema(c.tableName, cols.toList)
    val manager: DDFManager = this.getManager
    val typeSpecs: Array[Class[_]] = Array(classOf[String])
    val ddf = manager.newDDF("test", typeSpecs, theDDF.getNamespace, c
      .tableName, schema)
    ddf
  }


  protected def select2ddf(ddf: DDF, s: Select, optionalSchema: Option[Schema] = None): DDF = {
    s.validate
    val ddf = this.getManager.getDDFByName(s.relations.head.getTableName)
    val typeSpecs: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Row])
    val table: DataSet[Row] = ddf.getRepresentationHandler.get(typeSpecs: _*).asInstanceOf[DataSet[Row]]
    val where = s.where.orNull
    val group = s.group.orNull

    val (cols, joined: DataSet[Row]) = join(ddf, typeSpecs, table, s)

    val filtered: DataSet[Row] = if (!isNull(where)) joined.where(where.expression) else joined
    val projected: DataSet[Row] = if (!isNull(group)) {
      if (s.project.isStar) {
        filtered.groupBy(group.expression: _*).select(ddf.getSchema.getColumnNames.mkString(","))
      } else {
        filtered.groupBy(group.expression: _*).select(s.project.asInstanceOf[ExprProjection].expressions: _*)
      }
    } else {
      project(s, ddf, filtered)
    }

    val schemaCols = if (cols.isEmpty) {
      def typeInfo: RowTypeInfo = projected.getType() match {
        case r: RenamingProxyTypeInfo[Row] => r.getUnderlyingType.asInstanceOf[RowTypeInfo]
        case t: RowTypeInfo => t
      }
      Column2RowTypeInfo.getColumns(typeInfo).toList
    } else {
      cols
    }

    val tableName = ddf.getSchemaHandler.newTableName
    val schema = optionalSchema.getOrElse(new Schema(tableName, schemaCols))
    //now order by and limit
    val sorted: DataSet[Row] = sort(s, projected, schema)
    val finalDataSet = limit(s, sorted)

    val manager: DDFManager = this.getManager
    val newDDF = manager.newDDF(finalDataSet, dsrTypeSpecs, theDDF.getNamespace, tableName, schema)
    newDDF
  }


  def limit(s: Select, dataSet: DataSet[Row]): DataSet[Row] = {
    if (s.limit > 0) dataSet.first(s.limit) else dataSet
  }

  def project(s: Select, ddf: DDF, dataSet: DataSet[Row]): DataSet[Row] = {
    if (s.project.isStar) {
      dataSet.select(ddf.getSchema.getColumnNames.mkString(","))
    } else {
      dataSet.select(s.project.asInstanceOf[ExprProjection].expressions: _*)
    }
  }

  def sort(s: Select, dataSet: DataSet[Row], schema: Schema): DataSet[Row] = {
    s.order.map { ord =>
      val orderByFields = ord.columns.map(_._1)
      val order: Array[Boolean] = ord.columns.map(_._2).toArray
      Sorts.sort(dataSet, schema, orderByFields, order)
    }.getOrElse(dataSet)
  }

  def join(ddf: DDF, typeSpecs: Array[Class[_]], table: DataSet[Row], s: Select): (List[Column], DataSet[Row]) = {
    val joins = s.relations.filter(i => i.isInstanceOf[JoinRelation])
    var cols: List[Column] = List.empty[Column]
    var joined = table
    joins.foreach { j =>
      val join = j.asInstanceOf[JoinRelation]
      val ddf2 = this.getManager.getDDFByName(join.withTableName)
      val table2: DataSet[Row] = ddf2.getRepresentationHandler.get(typeSpecs: _*).asInstanceOf[DataSet[Row]]
      val (newCols, newDS) = Joins.joinDataSets(join.joinType, List.empty[String],
        join.joinCondition.left, join.joinCondition.right,
        table, table2,
        ddf.getSchema, ddf2.getSchema)
      cols = newCols.toList
      joined = newDS
    }
    (cols, joined)
  }

  //This implementation ignores dataSourceDescriptor and dataFormat as they are not valid for Flink (specific to Spark DataSource API)
  private def internalSql2ddf(command: String,
                              optionalSchema: Option[Schema] = None,
                              optionalDataSource: Option[DataSourceDescriptor] = None,
                              optionalDataFormat: Option[DataFormat] = None): DDF = {
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
        val newDDF = select2ddf(theDDF, s, optionalSchema)
        this.getManager.addDDF(newDDF)
        newDDF
    }
  }

  //This is required as ASqlHandler.sql2ddfHandle from ddf-core only supports Spark Engine
  override def sql2ddfHandle(command: String,
                             schema: Schema,
                             dataSource: DataSourceDescriptor,
                             dataFormat: DataFormat,
                             tableNameReplacer: TableNameReplacer): DDF = {
    internalSql2ddf(command, Option(schema), Option(dataSource), Option(dataFormat))
  }

  override def sql2ddf(command: String,
                       schema: Schema,
                       dataSource: DataSourceDescriptor,
                       dataFormat: DataFormat): DDF = {
    internalSql2ddf(command, Option(schema), Option(dataSource), Option(dataFormat))
  }

  override def sql2ddf(command: String,
                       schema: Schema,
                       dataSourceDescriptor: DataSourceDescriptor): DDF = {
    internalSql2ddf(command, Option(schema), Option(dataSourceDescriptor))
  }

  override def sql2ddf(command: String,
                       schema: Schema,
                       dataFormat: DataFormat): DDF = {
    internalSql2ddf(command, Option(schema), optionalDataFormat = Option(dataFormat))
  }

  override def sql2ddf(command: String, dataFormat: DataFormat): DDF = {
    internalSql2ddf(command, optionalDataFormat = Option(dataFormat))
  }

  override def sql2ddf(command: String,
                       schema: Schema): DDF = {
    internalSql2ddf(command, Option(schema))
  }

  override def sql2ddf(command: String): DDF = internalSql2ddf(command)


  /**
   * Parses a given sql string statement and performs corresponding action.
   * The limit option is only considered for a select statement.
   * Note: the limit parameter has preference over the limit specified in the string query.
   * @param command
   * @param limit
   * @return schema and rows
   */
  def sql2List(command: String, limit: Option[Integer] = None): (Schema, util.List[String]) = {
    val fn = parse(command)
    fn match {
      case c: Create =>
        val ddf = create2ddf(c)
        this.getManager.addDDF(ddf)
        (ddf.getSchema, Collections.singletonList("0"))

      case l: Load =>
        val ddf = load2ddf(l)
        val table: Table = ddf.getRepresentationHandler.get(tTypeSpecs: _*).asInstanceOf[Table]
        (ddf.getSchema, Collections.singletonList("0")) //seqAsJavaList(table.collect().map(_.toString()))

      case s: Select =>

        val selectStmt = limit.map {
          resultLimit =>
            Select(s.project, s.relations, s.where, s.group, s.order, resultLimit)
        }.getOrElse(s)

        val ddf = select2ddf(theDDF, selectStmt)
        val table: Table = ddf.getRepresentationHandler.get(tTypeSpecs: _*).asInstanceOf[Table]
        (ddf.getSchema, seqAsJavaList(table.collect().map(_.toString())))
    }
  }


  val dsrTypeSpecs: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Row])
  val tTypeSpecs: Array[Class[_]] = Array(classOf[Table])
  val dsoTypeSpecs: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Array[Object]])

  private def internalSql(command: String,
                          optionalLimit: Option[Integer] = None,
                          optionalDataSource: Option[DataSourceDescriptor] = None): SqlResult = {
    val res = sql2List(command, optionalLimit)
    new SqlResult(res._1, res._2)
  }

  override def sql(command: String, maxRows: Integer, dataSource: DataSourceDescriptor): SqlResult = {
    internalSql(command, Option(maxRows), Option(dataSource))
  }

  override def sql(command: String, maxRows: Integer): SqlResult = {
    internalSql(command, Option(maxRows))
  }

  override def sql(command: String): SqlResult = {
    internalSql(command)
  }

  override def sqlHandle(sqlcmd: String,
                         maxRows: Integer,
                         dataSource: DataSourceDescriptor,
                         tableNameReplacer: TableNameReplacer): SqlResult = {
    sql(sqlcmd, maxRows, dataSource)
  }

  private def internalSqlTyped(command: String,
                               optionalLimit: Option[Integer] = None,
                               optionalDataSource: Option[DataSourceDescriptor] = None): SqlTypedResult = {
    val sqlResult = internalSql(command, optionalLimit, optionalDataSource)
    new SqlTypedResult(sqlResult)
  }

  override def sqlTyped(command: String, maxRows: Integer, dataSource: DataSourceDescriptor): SqlTypedResult = {
    internalSqlTyped(command, Option(maxRows), Option(dataSource))
  }

  override def sqlTyped(command: String, maxRows: Integer): SqlTypedResult = {
    internalSqlTyped(command, Option(maxRows))
  }

  override def sqlTyped(command: String): SqlTypedResult = {
    internalSqlTyped(command)
  }
}
