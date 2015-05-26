package io.flink.ddf

import java.util

import com.clearspring.analytics.stream.quantile.QDigest
import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.content.Schema.{ColumnType, Column}
import org.apache.flink.api.common.accumulators.Histogram
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala.{ExecutionEnvironment}
import org.apache.flink.api.table.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.{AbstractID, Collector}
import org.apache.flink.api.scala.{DataSet, _}
import scala.reflect.ClassTag
import scala.util.Try

package object utils {

  object Misc extends Serializable {
    /**
     * Convenience method to get the elements of a DataSet of Doubles as a Histogram
     *
     * @return A List containing the elements of the DataSet
     */
    def collectHistogram[T: ClassTag](env: ExecutionEnvironment, dataSet: DataSet[Double], bins: Array[java.lang.Double])(implicit typeInformation: TypeInformation[Histogram]): util.TreeMap[Double, Int] = {
      val id: String = new AbstractID().toString
      dataSet.flatMap(new HistogramHelper(id, bins)).output(new DiscardingOutputFormat)
      val res = env.execute()
      res.getAccumulatorResult(id)
    }


    /**
     *
     * @param id
     * @param bins Can be null for a full histogram
     */
    class HistogramHelper(id: String, bins: Array[java.lang.Double]) extends RichFlatMapFunction[Double, Histogram] {
      private var accumulator: HistogramForDouble = null

      @throws(classOf[Exception])
      override def open(parameters: Configuration) {
        if (bins == null) this.accumulator = new HistogramForDouble()
        else this.accumulator = new HistogramForDouble(bins)
        getRuntimeContext.addAccumulator(id, accumulator)
      }

      override def flatMap(in: Double, collector: Collector[Histogram]): Unit = this.accumulator.add(in)
    }

    def qDigest(iterator: Iterator[Double]) = {
      val qDigest = new QDigest(100)
      iterator.foreach(i=> qDigest.offer(i.toLong))
      Array(qDigest)
    }

   def getDoubleColumn(ddf:DDF,columnName: String): Option[DataSet[Double]] = {
      val schema = ddf.getSchema
      val column: Column = schema.getColumn(columnName)
      column.isNumeric match {
        case true =>
          val data: DataSet[Array[Object]] = ddf.getRepresentationHandler.get(classOf[DataSet[_]],classOf[Array[Object]]).asInstanceOf[DataSet[Array[Object]]]
          val colIndex = ddf.getSchema.getColumnIndex(columnName)
          val colData = data.map {
            x =>
              val elem = x(colIndex)
              val mayBeDouble = Try(elem.toString.trim.toDouble)
              mayBeDouble.getOrElse(0.0)
          }
          Option(colData)
        case false => Option.empty[DataSet[Double]]
      }
    }

    def getBinned(ddf:DDF,b:Array[Double],col: String, intervals: Array[String], includeLowest: Boolean, right: Boolean): DDF = {
      val schema = ddf.getSchema
      val column = schema.getColumn(col)
      val colIndex = schema.getColumnIndex(col)
      val data: DataSet[Array[Object]] = ddf.getRepresentationHandler.get(classOf[DataSet[_]], classOf[Array[Object]]).asInstanceOf[DataSet[Array[Object]]]


      def getInterval(value: Double,b:Array[Double]): String = {
        var interval: String = null
        //case lowest
        if (value >= b(0) && value <= b(1)) {
          if (right) {
            if (includeLowest) if (value >= b(0) && value <= b(1)) interval = intervals(0)
            else
            if (value > b(0) && value <= b(1)) interval = intervals(0)
          }
          else {
            if (value >= b(0) && value < b(1)) interval = intervals(0)
          }
        } else if (b(b.length - 2) >= value && value <= b(b.length - 1)) {
          //case highest
          if (right) {
            if (value > b(b.length - 2) && value <= b(b.length - 1)) interval = intervals(intervals.length - 1)
          }
          else {
            if (includeLowest) if (value >= b(b.length - 2) && value <= b(b.length - 1)) interval = intervals(intervals.length - 1)
            else
            if (value >= b(b.length - 2) && value < b(b.length - 1)) interval = intervals(intervals.length - 1)
          }
        } else {
          //case intermediate breaks
          (1 to b.length - 3).foreach { i =>
            if (right)
              if (value > b(i) && value <= b(i + 1)) interval = intervals(i)
              else
              if (value >= b(i) && value < b(i + 1)) interval = intervals(i)
          }
        }
        interval
      }

      val binned = data.map { row =>
        val elem = row(colIndex)
        val mayBeDouble = Try(elem.toString.trim.toDouble)
        val value = mayBeDouble.getOrElse(0.0)
        row(colIndex) = getInterval(value,b)
        row
      }

      val cols = schema.getColumns
      cols.set(colIndex, new Column(column.getName, ColumnType.STRING))
      val newTableName = ddf.getSchemaHandler.newTableName()
      val newSchema = new Schema(newTableName, cols)
      ddf.getManager().newDDF(binned, Array(classOf[DataSet[_]], classOf[Array[Object]]), null, newTableName, newSchema)
    }
  }

  object Joins {

    def merge(row1: Row, row2: Row, joinedColNames:Seq[Schema.Column], leftSchema:Schema,rightSchema:Schema) = {
      val row: Row = new Row(joinedColNames.size)
      val r1 = if(row1 == null) new Row(leftSchema.getNumColumns) else row1
      val r2 = if(row2 == null) new Row(rightSchema.getNumColumns) else row2
      var i = 0
      joinedColNames.foreach { colName =>
        val colIdx = leftSchema.getColumnIndex(colName.getName)
        if (colIdx > -1) {
          val obj = r1.productElement(colIdx)
          row.setField(i, obj)
        }
        else {
          val colIdx = rightSchema.getColumnIndex(colName.getName)
          val obj = r2.productElement(colIdx)
          row.setField(i, obj)
        }
        i = i + 1
      }
      row
    }

    def mergeIterator(iter: Iterator[(Row, Row)], joinedColNames:Seq[Schema.Column], leftSchema:Schema,rightSchema:Schema): Iterator[Row] = {
     if(iter!=null) iter.map { case (r1, r2) => merge(r1, r2,joinedColNames,leftSchema,rightSchema)} else List[Row]().iterator
    }
  }




}
