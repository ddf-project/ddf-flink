/*
 * Copyright 2014, Tuplejump Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.flink.ddf.analytics

import java.io.IOException
import java.{lang, util}

import com.google.common.collect.Lists
import io.ddf.DDF
import io.ddf.analytics.AStatisticsSupporter.{FiveNumSummary, HistogramBin}
import io.ddf.analytics.{AStatisticsSupporter, ISupportStatistics}
import io.flink.ddf.{FlinkDDF, FlinkDDFManager}
import io.flink.ddf.content.Conversions
import io.flink.ddf.utils.{CollectHelper, SerializedListAccumulator}
import org.apache.flink.api
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala.DataSet
import org.apache.flink.runtime.AbstractID
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

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
    val statsDS = dataSet.mapPartition(nums => Iterator(new StatCounter(nums))).reduce((a, b) => a.merge(b))
    collect[StatCounter](manager.getExecutionEnvironment, statsDS).get(0)
  }

  override def getVectorVariance(columnName: String): Array[lang.Double] = {
    val sd: Array[lang.Double] = new Array[lang.Double](2)
    sd(0) = stats(doubleDataSet(columnName)).sampleVariance
    sd(1) = Math.sqrt(sd(0));
    sd
  }


  def doubleDataSet(columnName: String): DataSet[Double] = {
    val dataSet: DataSet[Array[Object]] = scalaDataSet
    val index = theDDF.getSchema.getColumnIndex(columnName);
    val column = theDDF.getSchema.getColumn(index);
    val doubleDataSet: DataSet[Double] = dataSet.map(t => Conversions.asDouble(t(index), column.getType))
    doubleDataSet
  }

  override def getVectorHistogram(columnName: String, numBins: Int): util.List[HistogramBin] = {
    val hist = histogram(doubleDataSet(columnName), numBins)
    val bins: util.List[AStatisticsSupporter.HistogramBin] = Lists.newArrayList()
    val binSize = hist._1.length
    for (i <- 0 to binSize) {
      val bin: AStatisticsSupporter.HistogramBin = new AStatisticsSupporter.HistogramBin
      bin.setX(hist._1(i))
      bin.setY(hist._2(i))
      bins.add(bin)
    }
    bins
  }

  override def getVectorMean(columnName: String): lang.Double = {
    stats(doubleDataSet(columnName)).mean
  }

  override def getFiveNumSummary(columnNames: util.List[String]): Array[FiveNumSummary] = null

  override def getVectorQuantiles(columnName: String, percentiles: Array[lang.Double]): Array[lang.Double] = null

  override def getVectorCor(xColumnName: String, yColumnName: String): Double = 0

  override def getVectorCovariance(xColumnName: String, yColumnName: String): Double = 0

  /**
   * Convenience method to get the elements of a DataSet as a List
   * As DataSet can contain a lot of data, this method should be used with caution.
   *
   * @return A List containing the elements of the DataSet
   */
  @throws(classOf[Exception])
  def collect[T:ClassTag](env: ExecutionEnvironment, dataSet: DataSet[T])(implicit typeInformation: TypeInformation[T]): java.util.List[T] = {
    val id: String = new AbstractID().toString
    dataSet.flatMap(new CollectHelper(id, dataSet.getType.createSerializer)).output(new DiscardingOutputFormat)
    val res = env.execute()
    val accResult: util.ArrayList[Array[Byte]] = res.getAccumulatorResult(id)
    try {
      return SerializedListAccumulator.deserializeList(accResult, dataSet.getType.createSerializer)
    }
    catch {
      case e: ClassNotFoundException => {
        throw new RuntimeException("Cannot find type class of collected data type.", e)
      }
      case e: IOException => {
        throw new RuntimeException("Serialization error while de-serializing collected data", e)
      }
    }
  }


  /**
   * Compute a histogram of the data using bucketCount number of buckets evenly
   * spaced between the minimum and maximum of the RDD. For example if the min
   * value is 0 and the max is 100 and there are two buckets the resulting
   * buckets will be [0, 50) [50, 100]. bucketCount must be at least 1
   * If the RDD contains infinity, NaN throws an exception
   * If the elements in RDD do not vary (max == min) always returns a single bucket.
   */
  def histogram(dataSet: DataSet[Double], bucketCount: Int): Pair[Array[Double], Array[Long]] = {
    val manager = this.getManager.asInstanceOf[FlinkDDFManager]
    // Scala's built-in range has issues. See #SI-8782
    def customRange(min: Double, max: Double, steps: Int): IndexedSeq[Double] = {
      val span = max - min
      Range.Int(0, steps, 1).map(s => min + (s * span) / steps) :+ max
    }
    val toCollect = dataSet.mapPartition { items =>
      Iterator(items.foldRight(Double.NegativeInfinity,
        Double.PositiveInfinity)((e: Double, x: Pair[Double, Double]) =>
        (x._1.max(e), x._2.min(e))))
    }.reduce { (maxmin1, maxmin2) =>
      (maxmin1._1.max(maxmin2._1), maxmin1._2.min(maxmin2._2))
    }
    // Compute the minimum and the maximum
    val (max: Double, min: Double) = collect(manager.getExecutionEnvironment,toCollect).get(0)
    if (min.isNaN || max.isNaN || max.isInfinity || min.isInfinity) {
      throw new UnsupportedOperationException(
        "Histogram on either an empty DataSet or DataSet containing +/-infinity or NaN")
    }
    val range = if (min != max) {
      // Range.Double.inclusive(min, max, increment)
      // The above code doesn't always work. See Scala bug #SI-8782.
      // https://issues.scala-lang.org/browse/SI-8782
      customRange(min, max, bucketCount)
    } else {
      List(min, min)
    }
    val buckets = range.toArray
    (buckets, histogram(dataSet, buckets, true))
  }

  def histogram(dataSet: DataSet[Double],
                buckets: Array[Double],
                evenBuckets: Boolean = false): Array[Long] = {
    if (buckets.length < 2) {
      throw new IllegalArgumentException("buckets array must have at least two elements")
    }
    // The histogramPartition function computes the partail histogram for a given
    // partition. The provided bucketFunction determines which bucket in the array
    // to increment or returns None if there is no bucket. This is done so we can
    // specialize for uniformly distributed buckets and save the O(log n) binary
    // search cost.
    def histogramPartition(bucketFunction: (Double) => Option[Int])(iter: Iterator[Double]):
    Iterator[Array[Long]] = {
      val counters = new Array[Long](buckets.length - 1)
      while (iter.hasNext) {
        bucketFunction(iter.next()) match {
          case Some(x: Int) => {
            counters(x) += 1
          }
          case _ => {}
        }
      }
      Iterator(counters)
    }


    // Merge the counters.
    def mergeCounters(a1: Array[Long], a2: Array[Long]): Array[Long] = {
      a1.indices.foreach(i => a1(i) += a2(i))
      a1
    }

    // Basic bucket function. This works using Java's built in Array
    // binary search. Takes log(size(buckets))
    def basicBucketFunction(e: Double): Option[Int] = {
      val location = java.util.Arrays.binarySearch(buckets, e)
      if (location < 0) {
        // If the location is less than 0 then the insertion point in the array
        // to keep it sorted is -location-1
        val insertionPoint = -location - 1
        // If we have to insert before the first element or after the last one
        // its out of bounds.
        // We do this rather than buckets.lengthCompare(insertionPoint)
        // because Array[Double] fails to override it (for now).
        if (insertionPoint > 0 && insertionPoint < buckets.length) {
          Some(insertionPoint - 1)
        } else {
          None
        }
      } else if (location < buckets.length - 1) {
        // Exact match, just insert here
        Some(location)
      } else {
        // Exact match to the last element
        Some(location - 1)
      }
    }
    // Determine the bucket function in constant time. Requires that buckets are evenly spaced
    def fastBucketFunction(min: Double, max: Double, count: Int)(e: Double): Option[Int] = {
      // If our input is not a number unless the increment is also NaN then we fail fast
      if (e.isNaN || e < min || e > max) {
        None
      } else {
        // Compute ratio of e's distance along range to total range first, for better precision
        val bucketNumber = (((e - min) / (max - min)) * count).toInt
        // should be less than count, but will equal count if e == max, in which case
        // it's part of the last end-range-inclusive bucket, so return count-1
        Some(math.min(bucketNumber, count - 1))
      }
    }
    // Decide which bucket function to pass to histogramPartition. We decide here
    // rather than having a general function so that the decision need only be made
    // once rather than once per shard
    val bucketFunction = if (evenBuckets) {
      fastBucketFunction(buckets.head, buckets.last, buckets.length - 1) _
    } else {
      basicBucketFunction _
    }
    if (dataSet.getParallelism == 0) {
      new Array[Long](buckets.length - 1)
    } else {
      // reduce() requires a non-empty DataSet. This works because the mapPartition will make
      // non-empty partitions out of empty ones. But it doesn't handle the no-partitions case,
      // which is below
      val manager = this.getManager.asInstanceOf[FlinkDDFManager]

      val ds: DataSet[Array[Long]] = dataSet.mapPartition(histogramPartition(bucketFunction) _).reduce(mergeCounters _)
      collect(manager.getExecutionEnvironment, ds).get(0)
    }
  }


}
