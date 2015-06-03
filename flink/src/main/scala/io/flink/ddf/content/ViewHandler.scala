package io.flink.ddf.content

import java.util

import io.ddf.DDF
import io.ddf.content.ViewHandler.{Column, Expression}
import io.ddf.content.{ViewHandler => CoreViewHandler}

class ViewHandler(ddf: DDF) extends io.ddf.content.ViewHandler(ddf){
  override def getRandomSample(numSamples: Int, withReplacement: Boolean, seed: Int): util.List[Array[AnyRef]] = {
    null
  }

  override def getRandomSample(percent: Double, withReplacement: Boolean, seed: Int): DDF ={
    null
  }

  override def getDDF: DDF = super.getDDF

  override def setDDF(theDDF: DDF): Unit = super.setDDF(theDDF)
}
