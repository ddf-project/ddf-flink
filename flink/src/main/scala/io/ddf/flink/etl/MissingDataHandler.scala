package io.ddf.flink.etl

import java.security.SecureRandom
import java.text.SimpleDateFormat
import java.util

import io.ddf.flink.FlinkConstants
import io.ddf.{DDFManager, DDF}
import io.ddf.content.Schema
import io.ddf.content.Schema.{Column, ColumnType}
import io.ddf.etl.IHandleMissingData
import io.ddf.etl.IHandleMissingData.{Axis, FillMethod, NAChecking}
import io.ddf.exception.DDFException
import io.ddf.misc.ADDFFunctionalGroupHandler
import io.ddf.types.AggregateTypes.AggregateFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.api.table.Row
import io.ddf.flink.utils.Misc._

import scala.collection.JavaConversions._

class MissingDataHandler(ddf: DDF) extends ADDFFunctionalGroupHandler(ddf) with IHandleMissingData {

  val rowDataSet = ddf.getRepresentationHandler.get(classOf[DataSet[_]], classOf[Row]).asInstanceOf[DataSet[Row]]

  private def generateDDF[T](data: T, typeSpecs: Array[Class[_]], schemaColumns: List[Column]): DDF = {
    val rand: SecureRandom = new SecureRandom
    val tableName: String = "tbl" + String.valueOf(Math.abs(rand.nextLong))
    val schema: Schema = new Schema(tableName, schemaColumns)
    val manager: DDFManager = ddf.getManager
    manager.newDDF(data, typeSpecs, FlinkConstants.NAMESPACE,
      tableName, schema)
  }

  private def dropColumnsWithNA(columnNames: List[String], columnNamesString: String, threshold: Long): DDF = {
    val columnIndices = columnNames.map(ddf.getColumnIndex)
    val columnWiseData = rowDataSet.flatMap {
      r => columnIndices.map {
        index => (index, r.productElement(index))
      }
    }.groupBy { x => x._1 }

    val columnWiseCount = columnWiseData.reduceGroup {
      x =>
        val colIndex = x.toSeq.head._1
        val colValues = x.map {
          entry =>
            if (isNull(entry._2)) 1 else 0
        }
        val countOfNullValues = colValues.sum
        (colIndex, countOfNullValues)
    }

    val validColumns = columnWiseCount.filter {
      entry =>
        entry._2 < threshold
    }.collect().toMap.keys.map(ddf.getColumnName)

    val resultDDF = ddf.VIEWS.project(validColumns.toList)
    resultDDF
  }

  private def dropRowWithNA(columnNames: List[String], actualThreshold: Long): DDF = {
    val columnIndices = columnNames.map(ddf.getColumnIndex)
    val filteredData = rowDataSet.filter {
      row =>
        val countOfNullValues = columnIndices.map {
          index =>
            if (isNull(row.productElement(index))) 1 else 0
        }.sum
        countOfNullValues < actualThreshold
    }

    val schemaColumns = columnNames.map {
      colName => ddf.getColumn(colName)
    }
    generateDDF(filteredData, Array(classOf[DataSet[_]], classOf[Row]), schemaColumns)
  }

  def calculateThreshold(thresh: Long, how: NAChecking, maxValue: Long): Long = {

    val result = (thresh, how) match {
      case (0, NAChecking.ANY) => 1
      case (0, NAChecking.ALL) => maxValue
      case (t, _) if t > 0 => maxValue - thresh + 1
    }
    result
  }

  override def dropNA(axis: Axis, how: NAChecking, threshold: Long, columns: util.List[String]): DDF = {
    val columnNames: List[String] = columns match {
      case c if (isNull(c) || c.isEmpty) => ddf.getColumnNames.toList
      case _ => columns.toList
    }
    val columnNamesString = columnNames.mkString(",")

    val result: DDF = axis match {
      case Axis.ROW if (threshold > ddf.getNumColumns) =>
        throw new DDFException("Required number of non-NA values per row must be less than or equal the number of columns.")

      case Axis.ROW =>
        // discard rows where the count of null values in a row is greater than the threshold
        val actualThreshold = calculateThreshold(threshold, how, ddf.getNumColumns)
        dropRowWithNA(columnNames, actualThreshold)

      case Axis.COLUMN if (threshold > ddf.getNumRows) =>
        throw new DDFException("Required number of non-NA values per column must be less than or equal the number of rows.")

      case Axis.COLUMN =>
        // discard columns where the count of null values in that column is greater than the threshold
        val actualThreshold = calculateThreshold(threshold, how, ddf.getNumRows)
        dropColumnsWithNA(columnNames, columnNamesString, actualThreshold)

      case _ => throw new DDFException("Either choose Axis.ROW for row-based NA filtering or Axis.COLUMN for column-based NA filtering")
    }
    result
  }

  override def fillNA(value: String, method: FillMethod, limit: Long, function: AggregateFunction,
                      columnsToValues: util.Map[String, String], columns: util.List[String]): DDF = {

    Option(method) match {
      case None =>
        fillByValue(value, function, columnsToValues, columns)
      case Some(x) =>
        throw new DDFException("This has not been implemented yet.")
    }
  }

  private def fillByValue(value: String,
                          aggregateFunction: AggregateFunction,
                          columnsToValues: util.Map[String, String],
                          columns: util.List[String]): DDF = {
    val dataColumns = columns match {
      case c if (isNull(c) || c.isEmpty) => ddf.getSchema.getColumns.zipWithIndex.toList
      case _ => columns.map {
        cName =>
          val column = ddf.getColumn(cName)
          val index = ddf.getColumnIndex(cName)
          (column, index)
      }.toList
    }

    val schemaColumns = dataColumns.zipWithIndex.map {
      case ((col, oldIndex), currentIndex) =>
        val columnName: String = col.getName
        mLog.info(s"$value -- $columnsToValues -- $aggregateFunction")
        val defaultValue = if (!isNull(value)) {
          value
        } else if (!isNull(columnsToValues) && columnsToValues.containsKey(columnName)) {
          columnsToValues(columnName)
        } else if (!isNull(aggregateFunction) && col.isNumeric) {
          ddf.getAggregationHandler.aggregateOnColumn(aggregateFunction, columnName).toString
        } else {
          throw new DDFException("Invalid fill value")
        }
        SchemaColumn(columnName, col.getType, oldIndex, currentIndex, defaultValue)
    }

    val dateFormat = new SimpleDateFormat()

    implicit val typeInfo: TypeInformation[Row] = rowDataSet.getType()
    val result: DataSet[Row] = rowDataSet.map {
      row =>
        val generatedRow = new Row(schemaColumns.length)

        schemaColumns.foreach {
          case col =>
            val index = col.currentIndex
            Option(row.productElement(col.oldIndex)) match {
              case None =>
                val replaceWithValue = col.default
                col.cType match {
                  case ColumnType.STRING => generatedRow.setField(index, replaceWithValue)
                  case ColumnType.INT => generatedRow.setField(index, replaceWithValue.toInt)
                  //case ColumnType.LONG => generatedRow.setField(index, replaceWithValue.toLong)
                  case ColumnType.FLOAT => generatedRow.setField(index, replaceWithValue.toFloat)
                  case ColumnType.DOUBLE => generatedRow.setField(index, replaceWithValue.toDouble)
                  case ColumnType.BIGINT => generatedRow.setField(index, replaceWithValue.toDouble)
                  case ColumnType.TIMESTAMP => generatedRow.setField(index, dateFormat.parse(replaceWithValue))
                  case ColumnType.BOOLEAN => generatedRow.setField(index, replaceWithValue.toBoolean)
                }
              case Some(x) =>
                generatedRow.setField(index, x)
            }
        }
        generatedRow
    }

    generateDDF(result, Array(classOf[DataSet[_]], classOf[Row]), schemaColumns.map {
      col => new Column(col.name, col.cType)
    })
  }
}
