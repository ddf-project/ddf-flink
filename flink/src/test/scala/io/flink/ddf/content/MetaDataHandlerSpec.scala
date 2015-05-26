package io.flink.ddf.content

import io.flink.ddf.BaseSpec

class MetaDataHandlerSpec extends BaseSpec {
  it should "get number of rows" in {
    ddf.getNumRows should be(31)
  }
}
