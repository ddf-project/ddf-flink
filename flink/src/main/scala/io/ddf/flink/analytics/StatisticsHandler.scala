package io.ddf.flink.analytics

import java.lang.Iterable
import java.{lang, util}

import com.clearspring.analytics.stream.quantile.QDigest
import io.ddf.DDF
import io.ddf.analytics.AStatisticsSupporter.FiveNumSummary
import io.ddf.analytics.{SimpleSummary, AStatisticsSupporter, Summary}
import io.ddf.content.Schema.Column
import io.ddf.flink.FlinkDDFManager
import io.ddf.flink.utils.Misc
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

  private def getDoubleColumn(columnName: String): Option[DataSet[Double]] = Misc.getDoubleColumn(ddf,columnName)

  override protected def getSummaryImpl: Array[Summary] = {
    val data: DataSet[Array[Object]] = ddf.getRepresentationHandler.get(classOf[DataSet[_]],classOf[Array[Object]]).asInstanceOf[DataSet[Array[Object]]]
    val colSize = ddf.getColumnNames.size()

    val colData: GroupedDataSet[(Int, String)] = data.flatMap { row =>
      (0 to colSize - 1).map {
        index =>
          //map each entry with its column index
          val elem = row(index)
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

  override def getVectorCor(xColumnName: String, yColumnName: String): Double = {
    ddf.getAggregationHandler.computeCorrelation(xColumnName,yColumnName)
  }

  override def getVectorCovariance(xColumnName: String, yColumnName: String): Double = {
    val manager = this.getManager.asInstanceOf[FlinkDDFManager]
    val dataSet: DataSet[Row] = ddf.getRepresentationHandler.get(classOf[DataSet[_]],classOf[Row]).asInstanceOf[DataSet[Row]]
    val xIndex = ddf.getSchema.getColumnIndex(xColumnName)
    val yIndex = ddf.getSchema.getColumnIndex(yColumnName)
    Misc.collectCovariance(manager.getExecutionEnvironment,dataSet,xIndex,yIndex)
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

  //TODO
  override protected def getSimpleSummaryImpl: Array[SimpleSummary] = ???
}



