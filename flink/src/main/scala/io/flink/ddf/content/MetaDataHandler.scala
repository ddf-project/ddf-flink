package io.flink.ddf.content

import io.ddf.DDF
import io.ddf.content.AMetaDataHandler
import io.flink.ddf.FlinkDDF
import org.apache.flink.api.scala.DataSet

class MetaDataHandler(ddf: DDF) extends AMetaDataHandler(ddf) {
  override protected def getNumRowsImpl: Long = {
    val dataset: DataSet[Array[Object]] = ddf.asInstanceOf[FlinkDDF].getDataSet
    dataset.count()
  }
}
