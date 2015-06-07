package io.ddf.flink.etl

import java.util

import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.etl.IHandleJoins
import io.ddf.etl.Types.JoinType
import io.ddf.flink.utils.Joins
import io.ddf.misc.ADDFFunctionalGroupHandler
import org.apache.flink.api.scala._
import org.apache.flink.api.table.Row

import scala.collection.JavaConversions._

class JoinHandler(ddf: DDF) extends ADDFFunctionalGroupHandler(ddf) with IHandleJoins {

  override def join(anotherDDF: DDF, joinType: JoinType, byColumns: util.List[String], byLeftColumns: util.List[String], byRightColumns: util.List[String]): DDF = {
    val leftTable = getDDF.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Row]): _*).asInstanceOf[DataSet[Row]]
    val rightTable = anotherDDF.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Row]): _*).asInstanceOf[DataSet[Row]]
    val leftSchema = getDDF.getSchema
    val rightSchema = anotherDDF.getSchema
    val (joinedColNames: Seq[Column], joinedDataSet: DataSet[Row]) = Joins.joinDataSets(joinType, byColumns, byLeftColumns, byRightColumns, leftTable, rightTable, leftSchema, rightSchema)

    val newTableName = this.getDDF.getSchemaHandler.newTableName()
    val joinedSchema = new Schema(newTableName, joinedColNames.toList)
    val resultDDF: DDF = this.getManager.newDDF(joinedDataSet, Array(classOf[DataSet[_]], classOf[Row]), getDDF.getNamespace, newTableName, joinedSchema)
    this.getDDF.getManager.addDDF(resultDDF)
    resultDDF

  }

  override def merge(anotherDDF: DDF): DDF = {
    val leftTable = getDDF.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Row]): _*).asInstanceOf[DataSet[Row]]
    val rightTable = anotherDDF.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Row]): _*).asInstanceOf[DataSet[Row]]
    val leftSchema = getDDF.getSchema
    val rightSchema = anotherDDF.getSchema
    val resultDataSet:DataSet[Row] =
    if(leftSchema.getColumns.equals(rightSchema.getColumns)){
      // the columns are the same we can simple union.
      leftTable.union(rightTable)
    }else{
      throw new IllegalArgumentException("Union can be done on DDFs with same schema")
    }
    val newTableName = this.getDDF.getSchemaHandler.newTableName()
    val newSchema = new Schema(newTableName, leftSchema.getColumns)
    val resultDDF: DDF = this.getManager.newDDF(resultDataSet, Array(classOf[DataSet[_]], classOf[Row]), getDDF.getNamespace, newTableName, newSchema)
    this.getDDF.getManager.addDDF(resultDDF)
    resultDDF
  }
}
