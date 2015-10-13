package io.ddf.flink.analytics

import java.lang.Iterable
import java.{lang, util}

import com.clearspring.analytics.stream.quantile.{QDigest, TDigest}
import io.ddf.DDF
import io.ddf.analytics.AStatisticsSupporter.FiveNumSummary
import io.ddf.analytics._
import io.ddf.content.Schema
import io.ddf.content.Schema.{Column, ColumnType}
import io.ddf.exception.DDFException
import io.ddf.flink.FlinkDDFManager
import io.ddf.flink.utils.Misc
import io.ddf.flink.utils.Misc._
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.api.table.Row
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class SummaryReducerFn extends GroupReduceFunction[(Int, String), (Int, Summary)] {
  def computeSummary(numbers: Array[Double], countNA: Long): Summary = {
    var summary: Summary = new Summary()
    if (numbers.nonEmpty) {
      summary = new Summary(numbers.toArray)
      summary.addToNACount(countNA)
    }
    summary
  }

  override def reduce(iterable: Iterable[(Int, String)], collector: Collector[(Int, Summary)]): Unit = {
    var colIndex = 0
    var numbers = Seq.empty[Double]
    var countNA = 0
    iterable.foreach {
      case (colId, value) =>
        colIndex = colId
        //continue if value is not null
        if (!isNull(value)) {
          val mayBeDouble = Try(value.toDouble)
          mayBeDouble match {
            case Success(number) =>
              numbers = numbers :+ number
            case Failure(other) =>
              //if value is na increase countNA else ignore
              if (value.equalsIgnoreCase("NA")) {
                countNA += 1
              }
          }
        }
    }
    val summary = computeSummary(numbers.toArray, countNA)
    collector.collect((colIndex, summary))
  }
}

class StatisticsHandler(ddf: DDF) extends AStatisticsSupporter(ddf) {

  //TODO update this by computing Summary on single column
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
    val data: DataSet[Array[Object]] = ddf.getRepresentationHandler.get(classOf[DataSet[_]], classOf[Array[Object]]).asInstanceOf[DataSet[Array[Object]]]
    val colSize = ddf.getColumnNames.size()

    val colData: GroupedDataSet[(Int, String)] = data.flatMap { row =>
      (0 to colSize - 1).map {
        index =>
          //map each entry with its column index
          val elem = row(index)
          val elemOpt: String = Option(elem).map(_.toString.trim).orNull
          (index, elemOpt)
      }
    }.groupBy {
      //grouping column values by column index
      x => x._1
    }

    //compute summary for group and return it with the group key
    //the group key is required later on for sorting
    val result = colData.reduceGroup[(Int, Summary)](new SummaryReducerFn)

    //collect and sort by column index and return the summaries
    result.collect().sortBy(_._1).map(_._2).toArray
  }

  override def getFiveNumSummary(columnNames: util.List[String]): Array[FiveNumSummary] = {
    val percentiles: Array[java.lang.Double] = Array(0.0001, 0.25, 0.5001, 0.75, .9999)
    columnNames.map { columnName =>
      val quantiles = getVectorQuantiles(columnName, percentiles)
      // scalastyle:off magic.number
      new FiveNumSummary(quantiles(0), quantiles(1), quantiles(2), quantiles(3), quantiles(4))
      // scalastyle:on magic.number
    }.toArray
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
    val summary = getSummaryVector(columnName)
    summary.map {
      s =>
        val result: lang.Double = s.mean
        result
    }.orNull
  }

  override def getVectorCor(xColumnName: String, yColumnName: String): Double = {
    ddf.getAggregationHandler.computeCorrelation(xColumnName, yColumnName)
  }

  override def getVectorCovariance(xColumnName: String, yColumnName: String): Double = {
    val manager = this.getManager.asInstanceOf[FlinkDDFManager]
    val dataSet: DataSet[Row] = ddf.getRepresentationHandler.get(classOf[DataSet[_]], classOf[Row]).asInstanceOf[DataSet[Row]]
    val xIndex = ddf.getSchema.getColumnIndex(xColumnName)
    val yIndex = ddf.getSchema.getColumnIndex(yColumnName)
    Misc.collectCovariance(manager.getExecutionEnvironment, dataSet, xIndex, yIndex)
  }

  private def getTDigest(columnName: String, percentiles: Array[lang.Double]): TDigest = {
    val maybeDataSet: Option[DataSet[Double]] = getDoubleColumn(columnName)

    maybeDataSet.map {
      ds =>
        val digests = ds.map {
          x =>
            val rs = new TDigest(Misc.compression)
            rs.add(x)
            rs
        }.reduce {
          (x, y) => x.add(y)
            x
        }
        val reducedList: util.List[TDigest] = digests.collect()
        val finalDigest = reducedList.reduce {
          (x, y) =>
            x.add(y)
            x
        }
        finalDigest
    }.orNull
  }

  private def getQDigest(columnName: String): QDigest = {
    val maybeDataSet: Option[DataSet[Double]] = getDoubleColumn(columnName)
    maybeDataSet.map {
      ds =>
        val digests = ds.mapPartition {
          x => Misc.qDigest(x)
        }.reduce {
          (x, y) => QDigest.unionOf(x, y)
        }
        val reducedList: util.List[QDigest] = digests.collect()
        val finalDigest = reducedList.reduce {
          (x, y) => QDigest.unionOf(x, y)
        }
        finalDigest
    }.orNull
  }


  override def getVectorQuantiles(columnName: String, percentiles: Array[lang.Double]): Array[lang.Double] = {
    val column = ddf.getColumn(columnName)
    if (ColumnType.isIntegral(column.getType)) {
      val qDigest = getQDigest(columnName)
      percentiles.map { p =>
        val quantile: lang.Double = qDigest.getQuantile(p).toDouble
        quantile
      }
    } else if (ColumnType.isFractional(column.getType)) {
      val tDigest = getTDigest(columnName, percentiles)
      percentiles.map {
        p =>
          val quantile: lang.Double = tDigest.quantile(p)
          quantile
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
    val notNullValues: DataSet[Array[Object]] = data.filter { d => !isNull(d(columnIndex)) }
    val tupleDataset = notNullValues.map {
      d =>
        val numericValue = d(columnIndex).toString.toDouble
        (numericValue, numericValue)
    }
    val aggregatedResult = tupleDataset.min(0).andMax(1)
    aggregatedResult.collect().headOption.getOrElse(defaultValue)
  }

}



