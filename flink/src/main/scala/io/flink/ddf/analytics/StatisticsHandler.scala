package io.flink.ddf.analytics

import java.lang.Iterable
import java.{lang, util}

import com.clearspring.analytics.stream.quantile.QDigest
import io.ddf.DDF
import io.ddf.analytics.AStatisticsSupporter.{FiveNumSummary, HistogramBin}
import io.ddf.analytics.{AStatisticsSupporter, Summary}
import io.ddf.content.Schema.Column
import io.flink.ddf.FlinkDDFManager
import io.flink.ddf.utils.Misc
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.api.table.Row
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class SummaryReducerFn extends GroupReduceFunction[(Int, String), (Int, Summary)] {
  def computeSummary(numbers: Array[Double], countNA: Long): Summary = {
    var summary: Summary = null
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
        if (value != null) {
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

  private def getDoubleColumn(columnName: String): Option[DataSet[Double]] = {
    val schema = ddf.getSchema
    val column: Column = schema.getColumn(columnName)
    column.isNumeric match {
      case true =>
        val data: DataSet[Row] = ddf.asInstanceOf[io.flink.ddf.FlinkDDF].getDataSet
        val colIndex = ddf.getSchema.getColumnIndex(columnName)
        val colData = data.map {
          x =>
            val elem = x.productElement(colIndex)
            val mayBeDouble = Try(elem.toString.trim.toDouble)
            mayBeDouble.getOrElse(0.0)
        }
        Option(colData)
      case false => Option.empty[DataSet[Double]]
    }
  }

  override protected def getSummaryImpl: Array[Summary] = {
    val data: DataSet[Row] = ddf.asInstanceOf[io.flink.ddf.FlinkDDF].getDataSet
    val colSize = ddf.getColumnNames.size()

    val colData: GroupedDataSet[(Int, String)] = data.flatMap { row =>
      (0 to colSize - 1).map {
        index =>
          //map each entry with its column index
          val elem = row.productElement(index)
          val elemOpt = if (elem == null) {
            null
          } else {
            elem.toString.trim
          }
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
      new FiveNumSummary(quantiles(0), quantiles(1), quantiles(2), quantiles(3), quantiles(4))
    }.toArray
  }

  override def getVectorVariance(columnName: String): Array[lang.Double] = {
    val mayBeSummary = getSummaryVector(columnName)
    mayBeSummary.isDefined match {
      case true =>
        val summary = mayBeSummary.get
        Array(summary.variance(), summary.stdev())
      case false => null
    }
  }

  override def getVectorMean(columnName: String): lang.Double = {
    val summary = getSummaryVector(columnName)
    summary.isDefined match {
      case true => summary.get.mean()
      case false => null
    }
  }

  //TODO
  override def getVectorCor(xColumnName: String, yColumnName: String): Double = 0

  //TODO
  override def getVectorCovariance(xColumnName: String, yColumnName: String): Double = 0

  override def getVectorHistogram(columnName: String, numBins: Int): util.List[HistogramBin] = {
    val columnData = getDoubleColumn(columnName).get
    val manager = this.getManager.asInstanceOf[FlinkDDFManager]

    val max = columnData.reduce(_ max _).collect().head
    val min = columnData.reduce(_ min _).collect().head

    // Scala's built-in range has issues. See #SI-8782
    def customRange(min: Double, max: Double, steps: Int): IndexedSeq[Double] = {
      val span = max - min
      Range.Int(0, steps - 1, 1).map(s => min + (s * span) / steps) :+ max
    }

    // Compute the minimum and the maximum
    if (min.isNaN || max.isNaN || max.isInfinity || min.isInfinity) {
      throw new UnsupportedOperationException(
        "Histogram on either an empty DataSet or DataSet containing +/-infinity or NaN")
    }
    val range = if (min != max) {
      // Range.Double.inclusive(min, max, increment)
      // The above code doesn't always work. See Scala bug #SI-8782.
      // https://issues.scala-lang.org/browse/SI-8782
      customRange(min, max, numBins)
    } else {
      List(min, min)
    }

    val buckets: Array[java.lang.Double] = range.toArray.map(i => Double.box(i))
    val bins: util.List[HistogramBin] = new util.ArrayList[HistogramBin]()
    val histograms = Misc.collectHistogram(manager.getExecutionEnvironment, columnData, buckets)
    histograms.foreach { entry =>
      val bin: AStatisticsSupporter.HistogramBin = new AStatisticsSupporter.HistogramBin
      bin.setX(entry._1)
      bin.setY(entry._2.toDouble)
      bins.add(bin)
    }
    bins
  }

  override def getVectorQuantiles(columnName: String, percentiles: Array[lang.Double]): Array[lang.Double] = {
    //TODO check for a better way - this implementation involves conversion from double to long and then long to double
    val maybeDataSet: Option[DataSet[Double]] = getDoubleColumn(columnName)
    maybeDataSet.isDefined match {
      case false => null
      case true =>
        val digests = maybeDataSet.get.mapPartition {
          x => Misc.qDigest(x)
        }.reduce {
          (x, y) => QDigest.unionOf(x, y)
        }
        val reducedList: util.List[QDigest] = digests.collect()
        val finalDigest = reducedList.reduce {
          (x, y) => QDigest.unionOf(x, y)
        }
        percentiles.map {
          i =>
            val quantile: lang.Double = finalDigest.getQuantile(i).toDouble
            quantile
        }
    }
  }
}



