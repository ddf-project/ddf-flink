package io.flink.ddf

import java.util

import com.clearspring.analytics.stream.quantile.QDigest
import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.content.Schema.{Column, ColumnType}
import io.ddf.etl.Types.JoinType
import io.flink.ddf.content.RepresentationHandler
import org.apache.flink.api.common.accumulators.{Accumulator, Histogram}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.api.table.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.{AbstractID, Collector}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.util.Try

package object utils {

  object Misc extends Serializable {


    class CovarianceCounter extends Accumulator[(Double, Double), java.lang.Double] {


      var xAvg = 0.0
      var yAvg = 0.0
      var Ck = 0.0
      var count = 0L

      // add an example to the calculation
      def add(x: Double, y: Double): this.type = {
        val oldX = xAvg
        count += 1
        xAvg += (x - xAvg) / count
        yAvg += (y - yAvg) / count
        Ck += (y - yAvg) * (x - oldX)
        this
      }

      // merge counters from other partitions. Formula can be found at:
      // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Covariance
      override def merge(acc: Accumulator[(Double,Double), java.lang.Double])= {
        val other = acc.asInstanceOf[CovarianceCounter]
        val totalCount = count + other.count
        Ck += other.Ck +
          (xAvg - other.xAvg) * (yAvg - other.yAvg) * count / totalCount * other.count
        xAvg = (xAvg * count + other.xAvg * other.count) / totalCount
        yAvg = (yAvg * count + other.yAvg * other.count) / totalCount
        count = totalCount
      }

      // return the sample covariance for the observed examples
      def cov: Double = Ck / (count - 1)


      override def getLocalValue: java.lang.Double = cov

      override def resetLocal(): Unit = {
        xAvg = 0.0
        yAvg = 0.0
        Ck = 0.0
        count = 0L
      }

      override def add(v: (Double, Double)): Unit = add(v._1,v._2)
    }


    class CovarianceHelper(id: String, xIndex: Int, yIndex: Int) extends RichFlatMapFunction[Row, CovarianceCounter] {
      private var accumulator: CovarianceCounter = null

      @throws(classOf[Exception])
      override def open(parameters: Configuration) {
        this.accumulator = new CovarianceCounter()
        trait Ser[M]
        implicit def toSer1[T <: AnyVal]: Ser[T] = new Ser[T] {}
        implicit def toSer2[T <: java.io.Serializable]: Ser[T] = new Ser[T] {}
        getRuntimeContext.addAccumulator(id, accumulator)
      }

      override def flatMap(in: Row, collector: Collector[CovarianceCounter]): Unit = {
        this.accumulator.add(asDouble(in.productElement(xIndex)), asDouble(in.productElement(yIndex)))
      }


      def asDouble(elem: Any) = {
        val mayBeDouble = Try(elem.toString.trim.toDouble)
        mayBeDouble.getOrElse(0.0)
      }
    }


    /**
     * Convenience method to get the elements of a DataSet of Doubles as a Histogram
     *
     * @return A List containing the elements of the DataSet
     */
    def collectCovariance[T: ClassTag](env: ExecutionEnvironment, dataSet: DataSet[Row], xIndex: Int, yIndex: Int)(implicit typeInformation: TypeInformation[CovarianceCounter]): Double = {
      val id: String = new AbstractID().toString
      dataSet.flatMap(new CovarianceHelper(id, xIndex, yIndex)).output(new DiscardingOutputFormat)
      val res = env.execute()
      res.getAccumulatorResult(id)
    }

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
      iterator.foreach(i => qDigest.offer(i.toLong))
      Array(qDigest)
    }

    def getDoubleColumn(ddf: DDF, columnName: String): Option[DataSet[Double]] = {
      val schema = ddf.getSchema
      val column: Column = schema.getColumn(columnName)
      column.isNumeric match {
        case true =>
          val data: DataSet[Array[Object]] = ddf.getRepresentationHandler.get(classOf[DataSet[_]], classOf[Array[Object]]).asInstanceOf[DataSet[Array[Object]]]
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

    def getBinned(ddf: DDF, b: Array[Double], col: String, intervals: Array[String], includeLowest: Boolean, right: Boolean): DDF = {
      val schema = ddf.getSchema
      val column = schema.getColumn(col)
      val colIndex = schema.getColumnIndex(col)
      val data: DataSet[Array[Object]] = ddf.getRepresentationHandler.get(classOf[DataSet[_]], classOf[Array[Object]]).asInstanceOf[DataSet[Array[Object]]]


      def getInterval(value: Double, b: Array[Double]): String = {
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
        row(colIndex) = getInterval(value, b)
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

    def joinDataSets(joinType: JoinType, byColumns: util.List[String], byLeftColumns:util.List[String], byRightColumns:util.List[String], leftTable: DataSet[Row], rightTable: DataSet[Row], leftSchema: Schema, rightSchema: Schema): (Seq[Column], DataSet[Row]) = {
      val joinCols = if (byColumns != null && byColumns.size > 0) collectionAsScalaIterable(byColumns).toArray else collectionAsScalaIterable(byLeftColumns).toArray
      val toCols = if (byColumns != null && byColumns.size > 0) collectionAsScalaIterable(byColumns).toArray else collectionAsScalaIterable(byRightColumns).toArray
      val leftCols = leftSchema.getColumns
      val rightCols = rightSchema.getColumns

      leftCols.foreach(i => rightCols.remove(leftCols))

      val isSemi = joinType == JoinType.LEFTSEMI
      val joinedColNames: Seq[Column] = if (isSemi) leftCols else leftCols.++(rightCols)

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

      val objArrDS= joinedDataSet.map {
        r =>
          val objArr: Array[Object] = (0 to joinedColNames.size -1).map { index =>
            r.productElement(index).asInstanceOf[Object]
          }.toArray
          objArr
      }

      (joinedColNames, RepresentationHandler.getRowDataSet(objArrDS,joinedColNames.toList))
    }


    def merge(row1: Row, row2: Row, joinedColNames: Seq[Schema.Column], leftSchema: Schema, rightSchema: Schema) = {
      val row: Row = new Row(joinedColNames.size)
      val r1 = if (row1 == null) new Row(leftSchema.getNumColumns) else row1
      val r2 = if (row2 == null) new Row(rightSchema.getNumColumns) else row2
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

    def mergeIterator(iter: Iterator[(Row, Row)], joinedColNames: Seq[Schema.Column], leftSchema: Schema, rightSchema: Schema): Iterator[Row] = {
      if (iter != null) iter.map { case (r1, r2) => merge(r1, r2, joinedColNames, leftSchema, rightSchema)} else List[Row]().iterator
    }
  }


}
