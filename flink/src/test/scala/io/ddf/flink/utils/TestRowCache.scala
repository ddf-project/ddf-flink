package io.ddf.flink.utils

import io.ddf.flink.BaseSpec

class RowCacheSpec extends BaseSpec {

  val filePath: String = getClass.getResource("/airline.csv").getPath

  it should "load and provide the dataset" in {
    val countFromFile = flinkDDFManager.getExecutionEnvironment.readTextFile(filePath).count()
    val countFromCache = ddf.getNumRows

    countFromFile should be(countFromCache)

  }
}
