package io.flink.ddf.content

import io.ddf.DDF
import io.ddf.content.AMetaDataHandler
import io.flink.ddf.FlinkDDFManager
import io.flink.ddf.utils._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.base.{LongComparator, LongSerializer}
import org.apache.flink.api.scala.DataSet
import scala.collection.JavaConverters._
import org.apache.flink.api.scala._

class MetaDataHandler(ddf: DDF) extends AMetaDataHandler(ddf) {
  override protected def getNumRowsImpl: Long = {
    val manager = this.getManager.asInstanceOf[FlinkDDFManager]
    val ds:DataSet[Array[Object]] = Misc.scalaDataSet(ddf)
    val dataSet = ds.map[Long]((i:Array[Object])=>1L).reduce(_+_)
    Misc.collect(manager.getExecutionEnvironment,dataSet).asScala.reduce(_+_)
  }

}
