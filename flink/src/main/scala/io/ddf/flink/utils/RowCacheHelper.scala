package io.ddf.flink.utils

import java.util

import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList
import org.apache.flink.api.common.accumulators.SerializedListAccumulator
import org.apache.flink.api.java.Utils.{CollectHelper, CountHelper}
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.typeinfo.RowTypeInfo
import org.apache.flink.util.AbstractID
import org.apache.flink.api.scala._

import scala.collection.JavaConverters._

object RowCacheHelper {

  def reloadRowsFromCache(flinkExecutionEnvironment: ExecutionEnvironment,fileURL: String, rowTypeInfo: RowTypeInfo, rdata: DataSet[Row]): DataSet[Row] = {
    implicit val ti: RowTypeInfo = rowTypeInfo

    val counter = new AbstractID().toString
    val collector = new AbstractID().toString

    val serializer = rowTypeInfo.createSerializer(flinkExecutionEnvironment.getConfig)

    rdata.flatMap(new CountHelper[Row](counter)).output(new DiscardingOutputFormat())
    rdata.flatMap(new CollectHelper[Row](collector, serializer)).output(new DiscardingOutputFormat())

    val res = flinkExecutionEnvironment.execute(s"Data Loader: $fileURL")

    val lsize: Long = res.getAccumulatorResult[Long](counter)

    val accResult: util.ArrayList[Array[Byte]] = res.getAccumulatorResult(collector)
    val ldata = SerializedListAccumulator.deserializeList(accResult, serializer).asScala.par



    val numSplits = rdata.getParallelism match {
      case num if num > 0 => num
      case _ => flinkExecutionEnvironment.getParallelism
    }

    val splitSize: Int = Math.ceil(lsize.toDouble / numSplits).toInt

    val splitAt = 1 to numSplits - 1 map {
      i => splitSize
    }


    val psplitLists = ldata.iterator.psplit(splitAt: _*)

    psplitLists.zipWithIndex.par.foreach {
      case (split, sidx) =>
        val bl = new ObjectBigArrayBigList[Row]()

        split.foreach(r =>
          bl.add(r)
        )

        RowCache.cacheSplit(fileURL, sidx.toString, bl)
    }

    val data = flinkExecutionEnvironment.createInput[Row](new RowCacheInputFormat(fileURL, numSplits))
    data
  }
}
