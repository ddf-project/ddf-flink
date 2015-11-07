package io.ddf.flink.analytics

import java.{lang, util}
import javax.print.attribute.standard.Compression

import com.clearspring.analytics.stream.quantile.TDigest
import io.ddf.DDF
import io.ddf.analytics.AStatisticsSupporter.FiveNumSummary
import io.ddf.analytics._
import io.ddf.content.Schema
import io.ddf.content.Schema.{Column, ColumnType}
import io.ddf.exception.DDFException
import io.ddf.flink.FlinkDDFManager
import io.ddf.flink.content.RepresentationHandler._
import io.ddf.flink.utils.Misc
import io.ddf.flink.utils.Misc._
import org.apache.flink.api.common.accumulators.{Accumulator, SimpleAccumulator}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.api.table.expressions.IsNull
import org.apache.flink.api.table.{Row, Table}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.{AbstractID, Collector}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class StatisticsHandler(ddf: DDF) extends AStatisticsSupporter(ddf) {

  private def getSummaryVector(columnName: String): Option[Summary] = {
    val schema = ddf.getSchema
    val column: Column = schema.getColumn(columnName)
    column.isNumeric match {
      case false => Option.empty[Summary]
      case true =>
        val colIndex = ddf.getSchema.getColumnIndex(columnName)
        val summaries = getSummary
        Option(summaries(colIndex))
    }
  }

  private def getDoubleColumn(columnName: String): Option[DataSet[Double]] = Misc.getDoubleColumn(ddf, columnName)

  override protected def getSummaryImpl: Array[Summary] = {
    val data: DataSet[Row] = ddf.getRepresentationHandler.get(DATASET_ROW_TYPE_SPECS: _*).asInstanceOf[DataSet[Row]]
    val columnNum = ddf.getSchema.getNumColumns
    val summaryDataset: DataSet[Array[Summary]] = data.mapPartition {
      rows =>
        val summaries = 0 to columnNum map(x => new Summary())
        rows.foreach {
          row =>
            (summaries, row.elementArray).zipped.map {
              case (summary, colValue: Int) if !isNull(colValue) =>
                summary.merge(colValue)
              case (summary, colValue: Long) if !isNull(colValue) =>
                summary.merge(colValue)
              case (summary, colValue: Float) if !isNull(colValue) =>
                summary.merge(colValue)
              case (summary, colValue: Double) if !isNull(colValue) =>
                summary.merge(colValue)
              case (td, _) =>
            }
        }
        Seq(summaries)
    }.reduce {
      (summary1, summary2) => (summary1, summary2).zipped.map {
        (x, y) =>
          x.merge(y)
      }
    }.map(_.toArray)

    summaryDataset.first(1).collect().head
  }

  override def getFiveNumSummary(columnNames: util.List[String]): Array[FiveNumSummary] = {
    val percentiles: Array[java.lang.Double] = Array(0.00001, 0.99999, 0.25, 0.5, 0.75)
    val data = ddf.getRepresentationHandler.get(DATASET_ROW_TYPE_SPECS: _*).asInstanceOf[DataSet[Row]]

    val tDigestDataset: DataSet[Array[TDigest]] = data.mapPartition {
      rows =>
        val tDigest = (0 to columnNames.size()-1).map(x => new TDigest(100)).toArray
        rows.foreach {
          row =>
            (tDigest, row.elementArray).zipped.map {
              case (td, colValue: Int) if !isNull(colValue) =>
                td.add(colValue)
              case (td, colValue: Float) if !isNull(colValue) =>
                td.add(colValue)
              case (td, colValue: Double) if !isNull(colValue) =>
                td.add(colValue)
              case (td, _) =>
            }
        }
        Seq(tDigest)
    }.reduce {
      (td1, td2) => (td1, td2).zipped.map {
        (x, y) =>
          if(y.size() > 0) x.add(y)
          x
      }
    }

    val tdigests = tDigestDataset.first(1).collect().head
    tdigests.map {
      td =>
        val quantiles = percentiles.map(p => if(td.size() > 0) td.quantile(p) else Double.NaN)
        new FiveNumSummary(quantiles(0), quantiles(1), quantiles(2), quantiles(3), quantiles(4))
    }
  }

  override def getVectorVariance(columnName: String): Array[lang.Double] = {
    val mayBeSummary = getSummaryVector(columnName)
    mayBeSummary.map {
      summary =>
        val result: Array[lang.Double] = Array(summary.variance(), summary.stdev())
        result
    }.orNull
  }

  override def getVectorMean(columnName: String): lang.Double = {
    val column = ddf.getColumn(columnName)
    if (column.isNumeric) {
      val table = ddf.getRepresentationHandler.get(TABLE_TYPE_SPECS: _*).asInstanceOf[Table]
      val row = table.select(s"$columnName.avg,$columnName").where(s"$columnName.isNotNull").first(1).collect().head
      row.productElement(0).toString.toDouble
    } else {
      throw new DDFException("Mean can only be computed on Numeric columns")
    }
  }

  override def getVectorCor(xColumnName: String, yColumnName: String): Double = {
    ddf.getAggregationHandler.computeCorrelation(xColumnName, yColumnName)
  }

  override def getVectorCovariance(xColumnName: String, yColumnName: String): Double = {
    val manager = this.getManager.asInstanceOf[FlinkDDFManager]
    val dataSet: DataSet[Row] = ddf.getRepresentationHandler.get(DATASET_ROW_TYPE_SPECS: _*).asInstanceOf[DataSet[Row]]
    val xIndex = ddf.getSchema.getColumnIndex(xColumnName)
    val yIndex = ddf.getSchema.getColumnIndex(yColumnName)
    Misc.collectCovariance(manager.getExecutionEnvironment, dataSet, xIndex, yIndex)
  }

  private def getTDigest(columnName: String): TDigest = {
    val table = ddf.getRepresentationHandler.get(DATASET_ROW_TYPE_SPECS: _*).asInstanceOf[DataSet[Row]]
    val columnData: DataSet[Row] = table.select(columnName).where(s"$columnName.isNotNull")
    val result = columnData.map {
      row =>
        val columnValue = row.productElement(0)
        val rs = new TDigest(100)
        if (!isNull(columnValue)) {
          rs.add(columnValue.toString.toDouble)
        }
        rs
    }.reduce {
      (x, y) =>
        x.add(y)
        x
    }.collect().head
    result
  }


  override def getVectorQuantiles(columnName: String, percentiles: Array[lang.Double]): Array[lang.Double] = {
    val column = ddf.getColumn(columnName)
    if (ColumnType.isNumeric(column.getType)) {
      val tDigest = getTDigest(columnName)
      percentiles.map { p =>
        val quantile = tDigest.quantile(p)
        val result: lang.Double = if (ColumnType.isIntegral(column.getType)) {
          quantile.floor
        } else {
          quantile
        }
        result
      }
    } else {
      throw new DDFException(new UnsupportedOperationException("Quantiles can only be calculated for numeric columns"))
    }
  }

  override protected def getSimpleSummaryImpl: Array[SimpleSummary] = {
    val ddfColumns: util.List[Column] = ddf.getSchema.getColumns
    val categoricalColumns = ddfColumns.filter(_.getColumnClass == Schema.ColumnClass.FACTOR)
    val numericalColumnTypes = Seq(ColumnType.BIGINT, ColumnType.DOUBLE, ColumnType.INT, ColumnType.FLOAT)
    val numericalColumns = ddfColumns.filter(c => numericalColumnTypes.contains(c.getType))

    val data: DataSet[Array[Object]] = ddf.getRepresentationHandler.get(classOf[DataSet[_]], classOf[Array[Object]]).asInstanceOf[DataSet[Array[Object]]]
    val categoricalSummaries: Array[CategoricalSimpleSummary] = categoricalColumns.map {
      col =>
        val columnIndex: Int = ddf.getColumnIndex(col.getName)
        val columnValues = data.map(d => Tuple1(d(columnIndex).toString)).distinct(0).collect().map(_._1).toList
        val categoricalSimpleSummary: CategoricalSimpleSummary = new CategoricalSimpleSummary()
        categoricalSimpleSummary.setColumnName(col.getName)
        categoricalSimpleSummary.setValues(columnValues)
        categoricalSimpleSummary
    }.toArray

    val numericalSummaries = numericalColumns.map {
      col =>
        val columnIndex: Int = ddf.getColumnIndex(col.getName)
        val numericalSummary = new NumericSimpleSummary()
        numericalSummary.setColumnName(col.getName)
        val (minValue, maxValue) = getMinAndMaxValue(data, columnIndex)
        numericalSummary.setMin(minValue)
        numericalSummary.setMax(maxValue)
        numericalSummary
    }
    categoricalSummaries ++ numericalSummaries
  }

  private def getMinAndMaxValue(data: DataSet[Array[Object]], columnIndex: Int): (Double, Double) = {
    val defaultValue = (Double.NaN, Double.NaN)
    val notNullValues: DataSet[Array[Object]] = data.filter {
      d => !isNull(d(columnIndex))
    }
    val tupleDataset = notNullValues.map {
      d =>
        val numericValue = d(columnIndex).toString.toDouble
        (numericValue, numericValue)
    }
    val aggregatedResult = tupleDataset.min(0).andMax(1)
    aggregatedResult.collect().headOption.getOrElse(defaultValue)
  }

}

class SummaryAccumulatorHelper(id: String, cols: Int) extends RichFlatMapFunction[Row, Summary] {
  private var summaryAccumulator: SummaryAccumulator = null

  override def open(parameters: Configuration): Unit = {
    summaryAccumulator = new SummaryAccumulator(cols)
    getRuntimeContext.addAccumulator(id, summaryAccumulator)
  }

  override def flatMap(in: Row, collector: Collector[Summary]): Unit = {
    summaryAccumulator.add(in)
  }
}

class SummaryAccumulator(cols: Int) extends Accumulator[Row, Array[Summary]] {
  private var summary: Array[Summary] = (1 to cols).map(x => new Summary()).toArray

  override def getLocalValue: Array[Summary] = summary

  override def resetLocal(): Unit = {
    summary = (1 to cols).map(x => new Summary()).toArray
  }

  override def merge(other: Accumulator[Row, Array[Summary]]): Unit = {
    this.summary = (this.summary, other.getLocalValue).zipped.map((r1col, r2col) => r1col.merge(r2col))
  }

  override def add(row: Row): Unit = {
    (this.summary, row.elementArray).zipped.foreach {
      case (s, entry) if !isNull(entry) =>
        entry match {
          case d: Double =>
            s.merge(d)
          case i: Int =>
            s.merge(i)
          case f: Float =>
            s.merge(f)
          case _ =>
            //if value is na increase countNA else ignore
            if (entry.toString.equalsIgnoreCase("NA")) {
              s.addToNACount(1)
            }
        }
      case (s, entry) if(isNull(entry)) =>
        //DO NOTHING
    }
  }
}


class TDigestHelper(id: String, cols: Int, compression: Int) extends RichFlatMapFunction[Row, Summary] {
  private var digestAccumulator: TDigestAccumulator = null

  override def open(parameters: Configuration): Unit = {
    digestAccumulator = new TDigestAccumulator(cols, compression)
    getRuntimeContext.addAccumulator(id, digestAccumulator)
  }

  override def flatMap(in: Row, collector: Collector[Summary]): Unit = {
    digestAccumulator.add(in)
  }
}

class TDigestAccumulator(cols: Int, compression: Int) extends Accumulator[Row, Array[TDigest]] {
  private var digests: Array[TDigest] = (0 to cols).map(x => new TDigest(compression)).toArray
  override def getLocalValue: Array[TDigest] = digests

  override def resetLocal(): Unit = {
    digests = (0 to cols).map(x => new TDigest(compression)).toArray
  }

  override def merge(other: Accumulator[Row, Array[TDigest]]): Unit = {
    (digests, other.getLocalValue).zipped.map{
      case (t1, t2) => if(t2.size() > 0) t1.add(t2)
    }
  }

  override def add(row: Row): Unit = {
    (digests, row.elementArray).zipped.map {
      case (td, element: Int) if !isNull(element) =>
        td.add(element)
      case (td, element: Long) if !isNull(element) =>
        td.add(element)
      case (td, element: Float) if !isNull(element) =>
        td.add(element)
      case (td, element: Double) if !isNull(element) =>
        td.add(element)
      case (td, _) =>
        //Do Nothing
    }
  }
}