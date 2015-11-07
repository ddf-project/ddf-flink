package io.ddf.flink.content

import io.ddf.DDF
import io.ddf.content.AMetaDataHandler
import io.ddf.flink.FlinkDDF
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.Row

class MetaDataHandler(ddf: DDF) extends AMetaDataHandler(ddf) {
  override protected def getNumRowsImpl: Long = {
    val dataset: DataSet[Row] = ddf.getRepresentationHandler.get(classOf[DataSet[_]],classOf[Row]).asInstanceOf[DataSet[Row]]
    dataset.count()
  }
}
