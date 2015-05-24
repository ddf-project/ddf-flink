package io.flink.ddf.analytics

import java.security.SecureRandom
import java.util

import io.ddf.DDF
import io.ddf.analytics.IHandleAggregation
import io.ddf.content.Schema
import io.ddf.content.Schema.{Column, ColumnType}
import io.ddf.misc.ADDFFunctionalGroupHandler
import io.ddf.types.AggregateTypes.{AggregateField, AggregateFunction, AggregationResult}
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.{Row, Table}

import scala.collection.JavaConversions._

class AggregationHandler(ddf: DDF) extends ADDFFunctionalGroupHandler(ddf) with IHandleAggregation {

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

  private def aggregateInternal[T](fields: Seq[AggregateField], f: (Table, Int, Int) => T): T = {
    val transformedFields: Seq[String] = getFields(fields)
    val groupByColumns: Seq[String] = fields.filterNot(_.isAggregated).map(_.getColumn)
    val table = ddf.getRepresentationHandler.get(classOf[Table]).asInstanceOf[Table]
    val transformedTable = groupByColumns.length match {
      case 0 => table
      case _ => table.groupBy(groupByColumns.mkString(","))
    }

    val fieldString: String = transformedFields.mkString(",")
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
    val table = ddf.getRepresentationHandler.get(classOf[Table]).asInstanceOf[Table]
    val row: Row = table.select(s"$col.$methodName").first(1).collect().head
    row.productElement(0).toString.toDouble
  }

  override def agg(aggregateFunctions: util.List[String]): DDF = {
    val fns: Seq[AggregateField] = aggregateFunctions.map(AggregateField.fromFieldSpec)
    val schema = ddf.getSchema
    aggregateInternal(fns, (table: Table, colSize: Int, numUnaggregatedFields: Int) => {
      val columns = fns.map {
        case x if x.isAggregated =>
          new Column(x.getAggregateFunction.toString(x.getColumn), ColumnType.DOUBLE)
        case other =>
          schema.getColumn(other.getColumn)
      }
      table2DDF(table, columns)
    })
  }

  override def xtabs(fields: util.List[AggregateField]): AggregationResult = {
    aggregate(fields)
  }

  override def computeCorrelation(columnA: String, columnB: String): Double = ???

  override def groupBy(groupedColumns: util.List[String], aggregateFunctions: util.List[String]): DDF = ???

  override def groupBy(groupedColumns: util.List[String]): DDF = ???

}
