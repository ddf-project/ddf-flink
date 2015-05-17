package io.flink.ddf.content

import io.ddf.DDF
import io.ddf.content.AMetaDataHandler
import io.flink.ddf.{FlinkDDF, FlinkDDFUtil}
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala._
import org.apache.flink.api.table.Row
import org.apache.flink.runtime.AbstractID

class MetaDataHandler(ddf: DDF) extends AMetaDataHandler(ddf) {
  override protected def getNumRowsImpl: Long = {
    val env: ExecutionEnvironment = FlinkDDFUtil.getEnv(ddf)
    val ds: DataSet[Row] = ddf.asInstanceOf[FlinkDDF].getDataSet
    val id: String = new AbstractID().toString

    ds.map(new CountHelper[Row](id)).output(new DiscardingOutputFormat)

    env.execute().getAccumulatorResult(id).asInstanceOf[Long]
  }
}
