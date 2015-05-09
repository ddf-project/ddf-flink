package io.flink.ddf.analytics

import io.ddf.DDF
import io.ddf.analytics.{AStatisticsSupporter, Summary}

class BasicStatisticsComputer(ddf: DDF) extends AStatisticsSupporter(ddf) {
  override protected def getSummaryImpl: Array[Summary] = ???
}
