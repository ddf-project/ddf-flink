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

import com.clearspring.analytics.stream.quantile.QDigest
import io.ddf.DDF
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala.DataSet
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.AbstractID
import org.apache.flink.util.Collector

import scala.reflect.ClassTag

/**
 * User: satya
 */
package object utils {

  object Misc extends Serializable {
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
      private var accumulator: Histogram = null

      @throws(classOf[Exception])
      override def open(parameters: Configuration) {
        if (bins == null) this.accumulator = new Histogram()
        else this.accumulator = new Histogram(bins)
        getRuntimeContext.addAccumulator(id, accumulator)
      }

      override def flatMap(in: Double, collector: Collector[Histogram]): Unit = this.accumulator.add(in)
    }



    def qDigest(iterator: Iterator[Double]) = {
      val qDigest = new QDigest(100)
      iterator.foreach(i=> qDigest.offer(i.toLong))
      Array(qDigest)
    }


    def scalaDataSet(theDDF:DDF): DataSet[Array[Object]] = {
      val flinkDDF = theDDF.asInstanceOf[FlinkDDF]
      new DataSet[Array[Object]](flinkDDF.getDataSetOfObjects)
    }

  }


}
