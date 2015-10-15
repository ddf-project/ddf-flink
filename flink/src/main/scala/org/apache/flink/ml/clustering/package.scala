//TODO remove this once KMeans is available in Flink ML
//Following code is required for KMeans implementation from https://github.com/apache/flink/pull/757

package org.apache.flink.ml

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.configuration.Configuration

import scala.reflect.ClassTag
import scala.collection.JavaConverters._

package object clustering {

  implicit class RichDataSetForKMeans[T](dataSet: DataSet[T]) {
    def mapWithBcSet[B, O: TypeInformation : ClassTag](
                                                        broadcastVariable: DataSet[B])(
                                                        fun: (T, Seq[B]) => O)
    : DataSet[O] = {
      dataSet.map(new BroadcastSetMapper[T, B, O](dataSet.clean(fun)))
        .withBroadcastSet(broadcastVariable, "broadcastVariable")
    }
  }

  private class BroadcastSetMapper[T, B, O](fun: (T, Seq[B]) => O)
    extends RichMapFunction[T, O] {
    var broadcastVariable: Seq[B] = _

    @throws(classOf[Exception])
    override def open(configuration: Configuration): Unit = {
      broadcastVariable = getRuntimeContext
        .getBroadcastVariable[B]("broadcastVariable")
        .asScala
        .toSeq
    }

    override def map(value: T): O = {
      fun(value, broadcastVariable)
    }
  }

}
