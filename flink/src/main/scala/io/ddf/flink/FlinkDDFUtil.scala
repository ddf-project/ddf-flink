package io.ddf.flink

import io.ddf.DDF

object FlinkDDFUtil {
  def getEnv(ddf: DDF) = {
    ddf.getManager.asInstanceOf[FlinkDDFManager].getExecutionEnvironment
  }
}

