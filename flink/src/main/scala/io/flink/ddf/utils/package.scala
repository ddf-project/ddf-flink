package io.flink.ddf

import java.util

import com.clearspring.analytics.stream.quantile.QDigest
import io.ddf.DDF
import org.apache.flink.api.common.accumulators.Histogram
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.{AbstractID, Collector}

import scala.reflect.ClassTag

package object utils {

  object Misc extends Serializable {
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
      private var accumulator: HistogramForDouble = null

      @throws(classOf[Exception])
      override def open(parameters: Configuration) {
        if (bins == null) this.accumulator = new HistogramForDouble()
        else this.accumulator = new HistogramForDouble(bins)
        getRuntimeContext.addAccumulator(id, accumulator)
      }

      override def flatMap(in: Double, collector: Collector[Histogram]): Unit = this.accumulator.add(in)
    }

    def qDigest(iterator: Iterator[Double]) = {
      val qDigest = new QDigest(100)
      iterator.foreach(i=> qDigest.offer(i.toLong))
      Array(qDigest)
    }


  }


}
