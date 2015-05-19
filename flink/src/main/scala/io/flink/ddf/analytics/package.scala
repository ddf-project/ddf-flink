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

package io.flink.ddf

import java.io.IOException
import java.util

import io.flink.ddf.utils.{CollectHelper, SerializedListAccumulator}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala.DataSet
import org.apache.flink.runtime.AbstractID

import scala.reflect.ClassTag

/**
 * User: satya
 */
package object analytics {

  object Stats extends Serializable {
    /**
     * Convenience method to get the elements of a DataSet as a List
     * As DataSet can contain a lot of data, this method should be used with caution.
     *
     * @return A List containing the elements of the DataSet
     */
    def collect[T: ClassTag](env: ExecutionEnvironment, dataSet: DataSet[T])(implicit typeInformation: TypeInformation[T]): java.util.List[T] = {
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
    def histogram(dataSet: DataSet[Double],
                  buckets: Array[Double],
                  evenBuckets: Boolean = false) = {
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
      (histogramPartition(bucketFunction) _, mergeCounters _)
    }

  }

}
