
package io.flink.ddf.analytics

import java.{lang, util}

import com.google.common.collect.Lists
import io.ddf.DDF
import io.ddf.analytics.AStatisticsSupporter.{FiveNumSummary, HistogramBin}
import io.ddf.analytics.{AStatisticsSupporter, ISupportStatistics}
import io.flink.ddf.content.Conversions
import io.flink.ddf.{FlinkDDF, FlinkDDFManager}
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{DataSet, _}

import scala.collection.JavaConverters._

/**
 * User: satya
 */
class StatsHandler(theDDF: DDF) extends BasicStatisticsComputer(theDDF) with ISupportStatistics {

  def scalaDataSet: DataSet[Array[Object]] = {
    val flinkDDF = theDDF.asInstanceOf[FlinkDDF]
    new DataSet[Array[Object]](flinkDDF.getDataSetOfObjects)
  }

  def stats(dataSet: DataSet[Double]): StatCounter = {
    val manager = this.getManager.asInstanceOf[FlinkDDFManager];
    val mapped = dataSet.map(i => new StatCounter(i))
    val reduced = mapped.reduce((a, b) => a.merge(b))
    val reducedList: util.List[StatCounter] = Stats.collect[StatCounter](manager.getExecutionEnvironment, reduced)
    println(reducedList)
    reducedList.get(0)
  }

  override def getVectorVariance(columnName: String): Array[lang.Double] = {
    val sd: Array[lang.Double] = new Array[lang.Double](2)
    sd(0) = stats(doubleDataSet(columnName)).sampleVariance
    sd(1) = Math.sqrt(sd(0));
    sd
  }


  override def getVectorMean(columnName: String): lang.Double = stats(doubleDataSet(columnName)).mean

  override def getFiveNumSummary(columnNames: util.List[String]): Array[FiveNumSummary] = null

  override def getVectorQuantiles(columnName: String, percentiles: Array[lang.Double]): Array[lang.Double] = null

  override def getVectorCor(xColumnName: String, yColumnName: String): Double = 0

  override def getVectorCovariance(xColumnName: String, yColumnName: String): Double = 0


  def doubleDataSet(columnName: String): DataSet[Double] = {
    val dataSet: DataSet[Array[Object]] = scalaDataSet
    val index = theDDF.getSchema.getColumnIndex(columnName);
    val column = theDDF.getSchema.getColumn(index);
    val doubleDataSet: DataSet[Double] = dataSet.map(t => Conversions.asDouble(t(index), column.getType))
    doubleDataSet
  }

  override def getVectorHistogram(columnName: String, numBins: Int): util.List[HistogramBin] = {
    val dataSet = doubleDataSet(columnName)
    val manager = this.getManager.asInstanceOf[FlinkDDFManager]
    val max = Stats.collect(manager.getExecutionEnvironment,dataSet.reduce(_ max _)).get(0)
    val min =  Stats.collect(manager.getExecutionEnvironment,dataSet.reduce(_ min _)).get(0)

    // Scala's built-in range has issues. See #SI-8782
    def customRange(min: Double, max: Double, steps: Int): IndexedSeq[Double] = {
      val span = max - min
      Range.Int(0, steps, 1).map(s => min + (s * span) / steps) :+ max
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
    val buckets = range.toArray

    val hs = Stats.histogram(dataSet, buckets, true)
    val ds: DataSet[Array[Long]] = dataSet.mapPartition(hs._1).reduce(hs._2)
    val hist = Stats.collect(manager.getExecutionEnvironment, ds).get(0)
    val bins: util.List[AStatisticsSupporter.HistogramBin] = Lists.newArrayList()
    var i: Int = 0
    while (i < hist.length) {
      val bin: AStatisticsSupporter.HistogramBin = new AStatisticsSupporter.HistogramBin
      bin.setX(buckets(i))
      bin.setY(hist(i))
      i += 1
      bins.add(bin)
    }

    bins.asScala.foreach(bin => print(bin.getX + "," + bin.getY))
    bins
  }


}
