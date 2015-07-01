package io.ddf.flink.content

import java.util
import java.util.{List, Map}

import io.ddf.content.{IHandleRepresentations, Schema}
import io.ddf.exception.DDFException
import io.ddf.{DDF, Factor}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._


class SchemaHandler(ddf:DDF) extends io.ddf.content.SchemaHandler(ddf){

  @throws(classOf[DDFException])
  override def computeFactorLevelsAndLevelCounts {
    val columnIndexes: util.List[Integer] = new util.ArrayList[Integer]
    val columnTypes: util.List[Schema.ColumnType] = new util.ArrayList[Schema.ColumnType]
    import scala.collection.JavaConversions._
    for (col <- this.getColumns) {
      if (col.getColumnClass eq Schema.ColumnClass.FACTOR) {
        var colFactor: Factor[_] = col.getOptionalFactor
        if (colFactor == null || colFactor.getLevelCounts == null || colFactor.getLevels == null) {
          if (colFactor == null) {
            colFactor = this.setAsFactor(col.getName)
          }
          columnIndexes.add(this.getColumnIndex(col.getName))
          columnTypes.add(col.getType)
        }
      }
    }
    var listLevelCounts: Map[Integer, Map[String, Integer]] = null
    if (columnIndexes.size > 0) {
      try {
          val dataSet:DataSet[Array[Object]] = this.ddf.getRepresentationHandler.get(RepresentationHandler.DATASET_ARR_OBJECT.getTypeSpecsString).asInstanceOf[DataSet[Array[Object]]]
          if (dataSet == null) {
            throw new DDFException("DataSet is null")
          }
          listLevelCounts = GetMultiFactor.getFactorCounts(dataSet, columnIndexes, columnTypes, classOf[Array[AnyRef]])
      }
      catch {
        case e: DDFException => {
          throw new DDFException("Error getting factor level counts", e)
        }
      }
      if (listLevelCounts == null) {
        throw new DDFException("Error getting factor levels counts")
      }
      import scala.collection.JavaConversions._
      for (colIndex <- columnIndexes) {
        val column: Schema.Column = this.getColumn(this.getColumnName(colIndex))
        val levelCounts: Map[String, Integer] = listLevelCounts.get(colIndex)
        if (levelCounts != null) {
          val factor: Factor[_] = column.getOptionalFactor
          val levels: List[String] = new util.ArrayList[String](levelCounts.keySet)
          factor.setLevelCounts(levelCounts)
          factor.setLevels(levels, false)
        }
      }
    }
  }
}
import java.lang.{Integer => JInt}
import java.util.{HashMap => JHMap, List => JList, Map => JMap}

import io.ddf.content.Schema.ColumnType
import io.ddf.exception.DDFException

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  */
object GetMultiFactor {

  //For Java interoperability
  def getFactorCounts[T](rdd: DataSet[T], columnIndexes: JList[JInt], columnTypes: JList[ColumnType], rddUnit: Class[T]):
  JMap[JInt, JMap[String, JInt]] = {

    getFactorCounts(rdd, columnIndexes, columnTypes)(ClassTag(rddUnit))
  }

  def getFactorCounts[T](rdd: DataSet[T], columnIndexes: JList[JInt], columnTypes: JList[ColumnType])(implicit tag: ClassTag[T]): JMap[JInt, JMap[String, JInt]] = {
    val columnIndexesWithTypes = (columnIndexes zip columnTypes).toList

    tag.runtimeClass match {
      case arrObj if arrObj == classOf[Array[Object]] =>
        val mapper = new ArrayObjectMultiFactorMapper(columnIndexesWithTypes)
        val rddArrObj:DataSet[Array[Object]] = rdd.asInstanceOf[DataSet[Array[Object]]]
        rddArrObj.mapPartition(mapper).reduce(new MultiFactorReducer).collect().head
      case _ =>
        throw new DDFException("Cannot get multi factor for RDD[%s]".format(tag.toString))
    }
  }


  class ArrayObjectMultiFactorMapper(indexsWithTypes: List[(JInt, ColumnType)])
    extends Function1[Iterator[Array[Object]], Iterator[JMap[JInt, JMap[String, JInt]]]] with Serializable {
    @Override
    def apply(iter: Iterator[Array[Object]]): Iterator[JMap[JInt, JMap[String, JInt]]] = {
      val aMap: JMap[JInt, JMap[String, JInt]] = new java.util.HashMap[JInt, JMap[String, JInt]]()
      while (iter.hasNext) {
        val row = iter.next()
        val typeIter = indexsWithTypes.iterator
        while (typeIter.hasNext) {
          val (idx, typ) = typeIter.next()
          val value: Option[String] = Option(row(idx)) match {
            case Some(x) => typ match {
              case ColumnType.INT => Option(x.asInstanceOf[Int].toString)
              case ColumnType.DOUBLE => Option(x.asInstanceOf[Double].toString)
              case ColumnType.STRING => Option(x.asInstanceOf[String])
              case ColumnType.FLOAT => Option(x.asInstanceOf[Float].toString)
              case ColumnType.BIGINT => Option(x.asInstanceOf[Long].toString)
              case unknown => x match {
                case y: java.lang.Integer => Option(y.toString)
                case y: java.lang.Double => Option(y.toString)
                case y: java.lang.String => Option(y)
                case y: java.lang.Long => Option(y.toString)
              }
            }
            case None => None
          }
          value match {
            case Some(string) => {
              Option(aMap.get(idx)) match {
                case Some(map) => {
                  val num = map.get(string)
                  map.put(string, if (num == null) 1 else num + 1)
                }
                case None => {
                  val newMap = new JHMap[String, JInt]()
                  newMap.put(string, 1)
                  aMap.put(idx, newMap)
                }
              }
            }
            case None =>
          }
        }
      }
      Iterator(aMap)
    }
  }



  class MultiFactorReducer
    extends Function2[JMap[JInt, JMap[String, JInt]], JMap[JInt, JMap[String, JInt]], JMap[JInt, JMap[String, JInt]]]
    with Serializable {
    @Override
    def apply(map1: JMap[JInt, JMap[String, JInt]], map2: JMap[JInt, JMap[String, JInt]]): JMap[JInt, JMap[String, JInt]] = {
      for ((idx, smap1) <- map1) {

        Option(map2.get(idx)) match {
          case Some(smap2) =>
            for ((string, num) <- smap1) {
              val aNum = smap2.get(string)
              smap2.put(string, if (aNum == null) num else (aNum + num))
            }
          case None => map2.put(idx, smap1)
        }
      }
      map2
    }
  }

}
