package io.flink.ddf.etl

import java.util

import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.etl.IHandleJoins
import io.ddf.etl.Types.JoinType
import io.ddf.exception.DDFException
import io.ddf.misc.ADDFFunctionalGroupHandler
import org.apache.flink.api.scala._
import org.apache.flink.api.table.Row

import scala.collection.JavaConversions._

class JoinHandler(ddf: DDF) extends ADDFFunctionalGroupHandler(ddf) with IHandleJoins {
  override def join(anotherDDF: DDF, joinType: JoinType, byColumns: util.List[String], byLeftColumns: util.List[String], byRightColumns: util.List[String]): DDF = {
    val leftTable = getDDF.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Row]): _*).asInstanceOf[DataSet[Row]]
    val rightTable = anotherDDF.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Row]): _*).asInstanceOf[DataSet[Row]]
    val joinCols = if (byColumns != null && byColumns.size() > 0) collectionAsScalaIterable(byColumns).toArray else collectionAsScalaIterable(byLeftColumns).toArray
    val toCols = if (byColumns != null && byColumns.size() > 0) collectionAsScalaIterable(byColumns).toArray else collectionAsScalaIterable(byRightColumns).toArray
    val isRight = joinType eq JoinType.RIGHT
    val joinedDataSet = try {
      if (isRight) {
        rightTable.join(leftTable).where(joinCols.head, joinCols.tail: _*).equalTo(toCols.head, toCols.tail: _*)
      }
      else {
        leftTable.join(rightTable).where(joinCols.head, joinCols.tail: _*).equalTo(toCols.head, toCols.tail: _*)
      }
    }
    catch {
      case ex: Exception => {
        throw new DDFException(String.format("Error while joinType.getStringRepr()"), ex)
      }
    }
    val leftSchema = getDDF.getSchema
    val rightSchema = anotherDDF.getSchema
    val leftCols = collectionAsScalaIterable(leftSchema.getColumns).toSet
    val rightCols = collectionAsScalaIterable(rightSchema.getColumns).toSet
    val joinedColNames = leftCols.union(rightCols)

    val newDS: DataSet[Row] = joinedDataSet.map[Row] { r: (Row, Row) =>
      val row: Row = new Row(joinedColNames.size)
      var i = 0
      joinedColNames.foreach { colName =>
        val colIdx = leftSchema.getColumnIndex(colName.getName)
        if (colIdx > -1) {
          val obj = if (isRight) r._2.productElement(colIdx) else r._1.productElement(colIdx)
          row.setField(i, obj)
        }
        else {
          val colIdx = rightSchema.getColumnIndex(colName.getName)
          val obj = if (isRight) r._1.productElement(colIdx) else r._2.productElement(colIdx)
          row.setField(i, obj)
        }
        i = i + 1
      }
      row
    }

    val newTableName = this.getDDF.getSchemaHandler.newTableName()
    val joinedSchema = new Schema(newTableName, joinedColNames.toList)
    val resultDDF: DDF = this.getManager.newDDF(newDS, Array(classOf[DataSet[_]], classOf[Row]), getDDF.getNamespace, newTableName, joinedSchema)
    this.getDDF.getManager.addDDF(resultDDF)
    resultDDF
  }
}
