package io.flink.ddf.analytics

import io.ddf.DDF
import io.ddf.analytics.{ABinningHandler, IHandleBinning}

class BinningHandler(mDDF: DDF) extends ABinningHandler(mDDF) with IHandleBinning {
  override def binningImpl(column: String, binningType: String, numBins: Int, breaks: Array[Double], includeLowest: Boolean, right: Boolean): DDF = ???
}
