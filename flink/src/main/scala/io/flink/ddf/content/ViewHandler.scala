package io.flink.ddf.content

import java.util

import io.ddf.DDF
import io.ddf.content.ViewHandler.{Expression, Column}
import io.ddf.content.{IHandleViews, ViewHandler => CoreViewHandler}

class ViewHandler(DDF: DDF) extends IHandleViews {
  override def getRandomSample(numSamples: Int, withReplacement: Boolean, seed: Int): util.List[Array[AnyRef]] = ???

  override def getRandomSample(percent: Double, withReplacement: Boolean, seed: Int): DDF = ???

  override def subset(columnExpr: util.List[Column], filter: Expression): DDF = ???

  override def top(numRows: Int, orderCols: String, mode: String): util.List[String] = ???

  override def project(columnNames: String*): DDF = ???

  override def project(columnNames: util.List[String]): DDF = ???

  override def removeColumns(columnNames: String*): DDF = ???

  override def removeColumns(columnNames: util.List[String]): DDF = ???

  override def removeColumn(columnName: String): DDF = ???

  override def head(numRows: Int): util.List[String] = ???

  override def getDDF: DDF = ???

  override def setDDF(theDDF: DDF): Unit = ???
}
