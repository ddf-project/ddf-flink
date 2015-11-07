package io.ddf.flink.utils

import it.unimi.dsi.fastutil.BigListIterator
import it.unimi.dsi.fastutil.objects.ObjectBigListIterators
import org.apache.flink.api.common.io.GenericInputFormat
import org.apache.flink.api.table.Row
import org.apache.flink.core.io.GenericInputSplit

class RowCacheInputFormat(filePath: String, maxpartitions: Int) extends GenericInputFormat[Row] {
  private var currentSplit: BigListIterator[Row] = null


  override def createInputSplits(numSplits: Int): Array[GenericInputSplit] = {
    0 to maxpartitions - 1 map (i => new GenericInputSplit(i, maxpartitions)) toArray
  }

  override def open(split: GenericInputSplit): Unit = {
    super.open(split)
    currentSplit = RowCache.getSplit(filePath, this.partitionNumber.toString) match {
      case Some(l) => l.listIterator()
      case None => ObjectBigListIterators.EMPTY_BIG_LIST_ITERATOR.asInstanceOf[BigListIterator[Row]]
    }
  }

  override def nextRecord(ot: Row): Row = {
    val r = currentSplit.next()
    r
  }

  override def reachedEnd(): Boolean = {
    !currentSplit.hasNext
  }
}
