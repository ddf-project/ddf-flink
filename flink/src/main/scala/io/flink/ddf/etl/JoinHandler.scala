package io.flink.ddf.etl

import java.util

import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.etl.IHandleJoins
import io.ddf.etl.Types.JoinType
import io.ddf.exception.DDFException
import io.ddf.misc.ADDFFunctionalGroupHandler
import io.flink.ddf.utils.Joins
import org.apache.flink.api.scala._
import org.apache.flink.api.table.Row

import scala.collection.JavaConversions._

class JoinHandler(ddf: DDF) extends ADDFFunctionalGroupHandler(ddf) with IHandleJoins {

  override def join(anotherDDF: DDF, joinType: JoinType, byColumns: util.List[String], byLeftColumns: util.List[String], byRightColumns: util.List[String]): DDF = {
    val leftTable = getDDF.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Row]): _*).asInstanceOf[DataSet[Row]]
    val rightTable = anotherDDF.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Row]): _*).asInstanceOf[DataSet[Row]]
    val joinCols = if (byColumns != null && byColumns.size() > 0) collectionAsScalaIterable(byColumns).toArray else collectionAsScalaIterable(byLeftColumns).toArray
    val toCols = if (byColumns != null && byColumns.size() > 0) collectionAsScalaIterable(byColumns).toArray else collectionAsScalaIterable(byRightColumns).toArray
    val leftSchema = getDDF.getSchema
    val rightSchema = anotherDDF.getSchema
    val leftCols = leftSchema.getColumns
    val rightCols = rightSchema.getColumns

    leftCols.foreach(i => rightCols.remove(leftCols))

    val isSemi = joinType == JoinType.LEFTSEMI
    val joinedColNames: Seq[Schema.Column] = if (isSemi) leftCols else leftCols.++(rightCols)

    val toCoGroup = leftTable.coGroup(rightTable).where(joinCols.head, joinCols.tail: _*).equalTo(toCols.head, toCols.tail: _*)
    val joinedDataSet: DataSet[Row] = joinType match {
      case JoinType.LEFT =>
        val coGroup = toCoGroup.apply { (leftTuples, rightTuples) =>
          //left outer join will have all left tuples even if right do not have a match in the coGroup
          if (rightTuples.isEmpty)
            for (left <- leftTuples) yield (left, null)
          else
            for (left <- leftTuples; right <- rightTuples) yield (left, right)

        }
        coGroup.flatMap(Joins.mergeIterator(_, joinedColNames, leftSchema, rightSchema))

      case JoinType.RIGHT =>
        val coGroup = toCoGroup.apply { (leftTuples, rightTuples) =>
          //right outer join will have all right tuples even if left do not have a match in the coGroup
          if (leftTuples.isEmpty)
            for (right <- rightTuples) yield (null, right)

          else
            for (left <- leftTuples; right <- rightTuples) yield (left, right)
        }
        coGroup.flatMap(Joins.mergeIterator(_, joinedColNames, leftSchema, rightSchema))

      case JoinType.FULL =>
        val coGroup = toCoGroup.apply { (leftTuples, rightTuples) =>
          //full outer join will have all right/left tuples even if left/right do not have a match in the coGroup
          if (rightTuples.isEmpty)
            for (left <- leftTuples) yield (left, null)
          else if (leftTuples.isEmpty)
            for (right <- rightTuples) yield (null, right)
          else
            for (left <- leftTuples; right <- rightTuples) yield (left, right)
        }
        coGroup.flatMap(Joins.mergeIterator(_, joinedColNames, leftSchema, rightSchema))

      case _ =>
        val coGroup = toCoGroup.apply { (leftTuples, rightTuples) =>
          //semi/inner join will only have tuples which have a match on both sides
          if (leftTuples.hasNext && rightTuples.hasNext)
            for (left <- leftTuples; right <- rightTuples) yield (left, right)
          else
            null
        }
        coGroup.flatMap(Joins.mergeIterator(_, joinedColNames, leftSchema, rightSchema))
    }

    val newTableName = this.getDDF.getSchemaHandler.newTableName()
    val joinedSchema = new Schema(newTableName, joinedColNames.toList)
    val resultDDF: DDF = this.getManager.newDDF(joinedDataSet, Array(classOf[DataSet[_]], classOf[Row]), getDDF.getNamespace, newTableName, joinedSchema)
    this.getDDF.getManager.addDDF(resultDDF)
    resultDDF

  }


}
