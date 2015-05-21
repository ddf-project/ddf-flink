package io.flink.ddf

import io.ddf.DDF

object FlinkDDFUtil {
  def getEnv(ddf: DDF) = {
    ddf.getManager.asInstanceOf[FlinkDDFManager].getExecutionEnvironment
  }
}

