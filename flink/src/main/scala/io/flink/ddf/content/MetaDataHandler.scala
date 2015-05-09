package io.flink.ddf.content

import io.ddf.DDF
import io.ddf.content.AMetaDataHandler

class MetaDataHandler(ddf: DDF) extends AMetaDataHandler(ddf) {
  override protected def getNumRowsImpl: Long = ???
}
