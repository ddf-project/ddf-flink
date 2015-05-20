package io.flink.ddf.analytics

import java.lang.Iterable

import io.ddf.DDF
import io.ddf.analytics.{AStatisticsSupporter, Summary}
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

class StatisticsComputer(ddf: DDF) extends AStatisticsSupporter(ddf) {

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

}



