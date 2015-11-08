package io.ddf.flink.utils

import com.google.common.collect.{Multimaps, Multimap, ArrayListMultimap}
import it.unimi.dsi.fastutil.BigList
import org.apache.flink.api.table.Row


import scala.collection.JavaConversions._
import scala.collection.mutable.{Map => MMap, Set => MSet}

object RowCache extends Serializable {
  private[flink] val cachedFiles: MMap[String, MSet[String]] = MMap.empty[String, MSet[String]]
  private[flink] val cacheMap: MMap[String, BigList[Row]] = MMap.empty[String, BigList[Row]]

  def cacheSplit(key: String, splitId: String, value: BigList[Row]): Unit = {
    cacheMap.put(s"$key-$splitId", value)
    cachedFiles.getOrElseUpdate(key, MSet[String]()).add(splitId)
  }

  def  getSplit(key: String, splitId: String) : Option[BigList[Row]] = {
    cacheMap.get(s"$key-$splitId")
  }

  def listSplits(key: String): MSet[String] = {
    cachedFiles.getOrElse(key, MSet.empty[String])
  }
}
