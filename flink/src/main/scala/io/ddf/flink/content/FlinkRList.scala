package io.ddf.flink.content

import org.rosuda.REngine.{REXP, RList}

case class FlinkRList(content: Array[REXP], names: Array[String])

object FlinkRList extends Serializable {
  def apply(rList: RList, names: Array[String]): FlinkRList = {
    val columnSize = names.length
    val content: Array[REXP] = (0 to columnSize - 1).map {
      index =>
        rList.at(index)
    }.toArray

    FlinkRList(content, names)
  }
}

