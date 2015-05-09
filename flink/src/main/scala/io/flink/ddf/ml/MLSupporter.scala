package io.flink.ddf.ml

import io.ddf.DDF
import io.ddf.ml.{MLSupporter => CoreMLSupporter}

class MLSupporter(ddf: DDF) extends CoreMLSupporter(ddf) with Serializable {

}
