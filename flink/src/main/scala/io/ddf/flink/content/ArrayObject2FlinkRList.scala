package io.ddf.flink.content

import io.ddf.DDF
import io.ddf.content.Schema.{Column, ColumnType}
import io.ddf.content.{ConvertFunction, Representation}
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import org.apache.flink.api.scala.{DataSet, _}
import org.rosuda.REngine._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try

class ArrayObject2FlinkRList(@transient ddf: DDF) extends ConvertFunction(ddf) {

  private def transformObjectArray2REXP(objArray: Seq[Object], column: Column): REXP = {
    val result: REXP =
      column.getType match {
        case ColumnType.STRING =>
          val updatedElems = objArray.map(elem => Try(elem.toString).getOrElse(null))
          new REXPString(updatedElems.toArray)
        case ColumnType.INT | ColumnType.LONG =>
          val updatedElems = objArray.map(elem => Try(elem.toString.toInt).getOrElse(REXPInteger.NA))
          new REXPInteger(updatedElems.toArray)
        case ColumnType.BIGINT | ColumnType.FLOAT | ColumnType.DOUBLE =>
          val updatedElems = objArray.map(elem => Try(elem.toString.toDouble).getOrElse(REXPDouble.NA))
          new REXPDouble(updatedElems.toArray)
      }
    result
  }

  override def apply(rep: Representation): Representation = {
    val repValue: Object = rep.getValue
    repValue match {
      case dataSet: DataSet[_] =>
        dataSet.getType() match {
          case x: ObjectArrayTypeInfo[_, _] =>
            val columns: List[Column] = ddf.getSchema.getColumns.toList

            val data: DataSet[Array[Object]] = dataSet.asInstanceOf[DataSet[Array[Object]]]
            val columnNames: Array[String] = ddf.getColumnNames.asScala.toArray

            val dataSetREXP: DataSet[FlinkRList] = data.mapPartition {
              pdata =>
                val subset = pdata.flatMap(x => x.zipWithIndex)
                val groupedData = subset.toSeq.groupBy(elem => elem._2)
                val rVectors = groupedData.map {
                  case (colIndex, colValues) =>
                    transformObjectArray2REXP(colValues.map(_._1), columns(colIndex))
                }.toArray

                val dfList = FlinkRList(rVectors, columnNames)
                Iterator(dfList)
            }

            new Representation(dataSetREXP, RepresentationHandler.DATASET_RList.getTypeSpecsString)
        }
    }
  }

}
