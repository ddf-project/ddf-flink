
package io.flink.ddf.analytics

import java.{lang, util}

import com.clearspring.analytics.stream.quantile.QDigest
import io.ddf.DDF
import io.ddf.analytics.AStatisticsSupporter.{FiveNumSummary, HistogramBin}
import io.ddf.analytics.{AStatisticsSupporter, ISupportStatistics}
import io.flink.ddf.content.Conversions
import io.flink.ddf.utils._
import io.flink.ddf.{FlinkDDFManager}
import org.apache.flink.api.scala.{DataSet, _}

import scala.collection.JavaConverters._

class StatsHandler(theDDF: DDF) extends BasicStatisticsComputer(theDDF) with ISupportStatistics {

  def doubleDataSet(columnName: String): DataSet[Double] = {
    val dataSet: DataSet[Array[Object]] = Misc.scalaDataSet(theDDF)
    val index = theDDF.getSchema.getColumnIndex(columnName);
    val column = theDDF.getSchema.getColumn(index);
    dataSet.map(t => Conversions.asDouble(t(index), column.getType))
  }

  override def getVectorVariance(columnName: String): Array[lang.Double] = {
    val sd: Array[lang.Double] = new Array[lang.Double](2)
    sd(0) = getSummary()(this.getDDF.getSchema.getColumnIndex(columnName)).variance
    sd(1) = Math.sqrt(sd(0));
    sd
  }


  override def getVectorMean(columnName: String): lang.Double = getSummary()(this.getDDF.getSchema.getColumnIndex(columnName)).mean

  override def getFiveNumSummary(columnNames: util.List[String]): Array[FiveNumSummary] = {
    val percentiles:Array[java.lang.Double] = Array(0.0001,0.25,0.5001,0.75,.9999)
    columnNames.asScala.map{ columnName =>
      val quantiles = getVectorQuantiles(columnName,percentiles)
      new FiveNumSummary(quantiles(0),quantiles(1),quantiles(2),quantiles(3),quantiles(4))
    }.toArray
  }

  override def getVectorQuantiles(columnName: String, percentiles: Array[lang.Double]): Array[lang.Double] ={
    val manager = this.getManager.asInstanceOf[FlinkDDFManager];
    val digests = doubleDataSet(columnName).mapPartition(Misc.qDigest( _)).reduce(QDigest.unionOf(_,_))
    val reducedList: util.List[QDigest] = Misc.collect[QDigest](manager.getExecutionEnvironment, digests)
    val finalDigest = reducedList.asScala.reduce(QDigest.unionOf(_,_))
    percentiles.map(i=> scalaToJavaDouble(finalDigest.getQuantile(i).toDouble))
  }

  //TODO
  override def getVectorCor(xColumnName: String, yColumnName: String): Double = 0

  //TODO
  override def getVectorCovariance(xColumnName: String, yColumnName: String): Double = 0

  override def getVectorHistogram(columnName: String, numBins: Int): util.List[HistogramBin] = {
    val dataSet = doubleDataSet(columnName)
    val manager = this.getManager.asInstanceOf[FlinkDDFManager]

    val max = Misc.collect(manager.getExecutionEnvironment, dataSet.reduce(_ max _)).get(0)
    val min = Misc.collect(manager.getExecutionEnvironment, dataSet.reduce(_ min _)).get(0)

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

    val buckets: Array[java.lang.Double] = range.toArray.map(i => scalaToJavaDouble(i))
    val bins: util.List[HistogramBin] = new util.ArrayList[HistogramBin]()
    var i: Int = 0
    Misc.collectHistogram(manager.getExecutionEnvironment, doubleDataSet(columnName), buckets)
      .asScala.foreach { entry =>
      val bin: AStatisticsSupporter.HistogramBin = new AStatisticsSupporter.HistogramBin
      bin.setX(entry._1)
      bin.setY(entry._2.toDouble)
      bins.add(bin)
    }
    bins
  }

  def scalaToJavaDouble(d: Double): java.lang.Double = d.doubleValue


}
