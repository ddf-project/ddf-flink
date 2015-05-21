package io.flink.ddf.content

import io.ddf.DDF
import io.ddf.content.AMetaDataHandler
import io.flink.ddf.FlinkDDF
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.Row

class MetaDataHandler(ddf: DDF) extends AMetaDataHandler(ddf) {
  override protected def getNumRowsImpl: Long = {
    val dataset: DataSet[Row] = ddf.asInstanceOf[FlinkDDF].getDataSet
    dataset.count()
  }
}
