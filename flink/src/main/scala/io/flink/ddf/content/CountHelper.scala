package io.flink.ddf.content

import org.apache.flink.api.common.functions.{RichMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.flink.api.common.accumulators.LongCounter

class CountHelper[T](id: String) extends RichMapFunction[T, T] {
  private var accumulator: LongCounter = null

  override def open(parameters: Configuration) {
    this.accumulator = new LongCounter
    getRuntimeContext.addAccumulator(id, accumulator)
  }

  def map(value: T): T = {
    accumulator.add(1L)
    value
  }
}
