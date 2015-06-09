package io.ddf.flink.analytics

import java.security.SecureRandom
import java.util

import io.ddf.DDF
import io.ddf.analytics.IHandleAggregation
import io.ddf.content.Schema
import io.ddf.content.Schema.{Column, ColumnType}
import io.ddf.exception.DDFException
import io.ddf.misc.ADDFFunctionalGroupHandler
import io.ddf.types.AggregateTypes.{AggregateField, AggregateFunction, AggregationResult}
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.{Row, Table}

import scala.collection.JavaConversions._

case class PearsonCorrelation(x: Double,
                              y: Double,
                              xy: Double,
                              x2: Double,
                              y2: Double,
                              count: Int) extends Serializable {

  def merge(other: PearsonCorrelation): PearsonCorrelation = {
    PearsonCorrelation(x + other.x,
      y + other.y,
      xy + other.xy,
      x2 + other.x2,
      y2 + other.y2,
      count + count
    )
  }

  def evaluate: Double = {
    val numr: Double = (count * xy) - (x * y)
    val base: Double = ((count * x2) - (x * x)) * ((count * y2) - (y * y))
    val denr: Double = Math.sqrt(base)
    val result: Double = numr / denr
    result
  }
}

class AggregationHandler(ddf: DDF) extends ADDFFunctionalGroupHandler(ddf) with IHandleAggregation {

  private var groupColumns: util.List[String] = List.empty[String]

  private def setGroups(groupedColumns: util.List[String]) = {
    groupColumns = groupedColumns
  }

  def getGroupColumns = groupColumns

  private def getMethodName(aggrFunction: AggregateFunction): String = {
    aggrFunction match {
      case af if af == AggregateFunction.MEAN || af == AggregateFunction.AVG => "avg"
      case AggregateFunction.MAX => "max"
      case AggregateFunction.MIN => "min"
      case AggregateFunction.SUM => "sum"
      //TODO check what happens for count(*)
      //      case AggregateFunction.COUNT => "count"
      case other => throw new RuntimeException(s"The method $other is currently not supported")
    }
  }

  private def getFields(fields: util.List[AggregateField]): Seq[String] = {
    fields.map {
      field =>
        if (field.isAggregated) {
          val methodName = getMethodName(field.getAggregateFunction)
          s"${field.getColumn}.$methodName"
        } else field.getColumn
    }
  }

  private def table2QueryResult(table: Table, colSize: Int): List[String] = {
    table.collect().map {
      row =>
        val rowStr: String = (0 to colSize).map {
          index =>
            row.productElement(index)
        }.mkString("\t")
        rowStr
    }.toList
  }

  private def table2DDF(table: Table, columns: Seq[Column]): DDF = {
    val rand: SecureRandom = new SecureRandom
    val tableName: String = "tbl" + String.valueOf(Math.abs(rand.nextLong))
    val generatedSchema = new Schema(tableName, columns)
    val typeSpecs: Array[Class[_]] = Array(classOf[Table])
    val resultDDF: DDF = ddf.getManager.newDDF(table, typeSpecs, ddf.getNamespace, tableName, generatedSchema)
    resultDDF
  }

  private def getCleanTable(columnNames: Seq[String]): Table = {
    val table = ddf.getRepresentationHandler.get(classOf[Table]).asInstanceOf[Table]
    val tableWithoutNull = table.select(columnNames.mkString(",")).where(columnNames.map{
      c => s"$c.isNotNull"
    }.mkString(" && "))
    tableWithoutNull
  }

  private def aggregateInternal[T](fields: Seq[AggregateField], f: (Table, Int, Int) => T): T = {
    val transformedFields: Seq[String] = getFields(fields)
    val fieldString: String = transformedFields.mkString(",")

    val groupByColumns: Seq[String] = fields.filterNot(_.isAggregated).map(_.getColumn)
    val columnNames: Seq[String] = fields.map(_.getColumn).seq
    val table = getCleanTable(columnNames)

    val transformedTable = groupByColumns.length match {
      case 0 => table
      case _ => table.groupBy(groupByColumns.mkString(","))
    }

    val selectedTable: Table = transformedTable.select(fieldString)

    val colSize = transformedFields.length - 1
    val numUnaggregatedFields = groupByColumns.length

    f(selectedTable, colSize, numUnaggregatedFields)
  }

  override def aggregate(fields: util.List[AggregateField]): AggregationResult = {
    aggregateInternal(fields, (table: Table, colSize: Int, numUnaggregatedFields: Int) => {
      val result: List[String] = table2QueryResult(table, colSize)
      AggregationResult.newInstance(result, numUnaggregatedFields)
    })
  }

  override def aggregateOnColumn(function: AggregateFunction, col: String): Double = {
    val methodName = getMethodName(function)
    val table = getCleanTable(Seq(col))
    val row: Row = table.select(s"${col.trim}.$methodName").first(1).collect().head
    row.productElement(0).toString.toDouble
  }

  private def stringToAggregateField(aggrFn: String): AggregateField = {
    val terms = aggrFn.split("=")
    if (terms.length > 1) {
      AggregateField.fromFieldSpec(terms(1).trim).setName(terms(0).trim)
    } else {
      AggregateField.fromFieldSpec(terms(0).trim)
    }
  }

  private def aggrFieldToSchemaColumn(fields: Seq[AggregateField]): Seq[Column] = {
    val schema = ddf.getSchema
    val columns = fields.map {
      case x if x.isAggregated =>
        new Column(x.getAggregateFunction.toString(x.getColumn), ColumnType.DOUBLE)
      case other =>
        schema.getColumn(other.getColumn)
    }
    columns
  }

  override def xtabs(fields: util.List[AggregateField]): AggregationResult = {
    aggregate(fields)
  }

  override def groupBy(groupedColumns: util.List[String], aggregateFunctions: util.List[String]): DDF = {
    val aggregateFields: util.List[AggregateField] = aggregateFunctions.map(stringToAggregateField)
    val table = getCleanTable(groupedColumns ++ aggregateFields.map(_.getColumn))
    val fields: Seq[String] = getFields(aggregateFields)
    val selectedColumns: Seq[String] = (fields ++ groupedColumns).distinct
    val groupedData = table.groupBy(groupedColumns.map(_.trim).mkString(",")).select(selectedColumns.mkString(","))
    val columns = aggrFieldToSchemaColumn(aggregateFields)
    val groupedSchemaColumns = groupedColumns.map(ddf.getSchema.getColumn)
    table2DDF(groupedData, (columns ++ groupedSchemaColumns).distinct)
  }

  override def computeCorrelation(columnA: String, columnB: String): Double = {
    val rowDataSet = ddf.getRepresentationHandler.get(classOf[DataSet[_]], classOf[Row]).asInstanceOf[DataSet[Row]]
    val colAIndex = ddf.getColumnIndex(columnA)
    val colBIndex = ddf.getColumnIndex(columnB)

    val intermediateResult = rowDataSet.map { row =>
      val colAElement = row.productElement(colAIndex).toString.toDouble
      val colBElement = row.productElement(colBIndex).toString.toDouble
      new PearsonCorrelation(colAElement, colBElement,
        colAElement * colBElement, colAElement * colAElement,
        colBElement * colBElement, 1)
    }.reduce(_.merge(_)).collect()

    val correlation = intermediateResult.reduce(_.merge(_)).evaluate
    correlation
  }

  override def groupBy(groupedColumns: util.List[String]): DDF = {
    val table = ddf.getRepresentationHandler.get(classOf[Table]).asInstanceOf[Table]
    val ddfCopy = table2DDF(table, ddf.getSchema.getColumns)
    val aggregationHandler = new AggregationHandler(ddfCopy)
    aggregationHandler.setGroups(groupedColumns)
    ddfCopy.setAggregationHandler(aggregationHandler)
    ddf.getManager.addDDF(ddfCopy)
    ddfCopy
  }

  //this method can only be called on a ddf whose AggregationHandler has groupColumns
  override def agg(aggregateFunctions: util.List[String]): DDF = {
    val handler = ddf.getAggregationHandler.asInstanceOf[AggregationHandler]
    val groupColumns = handler.getGroupColumns
    if (groupColumns.nonEmpty) {
      val resultDDF = groupBy(groupColumns, aggregateFunctions)
      val aggregationHandler = new AggregationHandler(resultDDF)
      resultDDF.setAggregationHandler(aggregationHandler)
      ddf.getManager.addDDF(resultDDF)
      resultDDF
    } else {
      throw new DDFException("Need to set grouped columns before aggregation")
    }
  }

}
