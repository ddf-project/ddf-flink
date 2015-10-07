package io.ddf.flink.content

import java.util

import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.content.ViewHandler.{Column, Expression, OperationName, Operator}
import io.ddf.flink.utils.Samples
import org.apache.flink.api.scala.DataSet

import scala.collection.JavaConversions._


class ViewHandler(ddf: DDF) extends io.ddf.content.ViewHandler(ddf) {
  override def getRandomSample(numSamples: Int, withReplacement: Boolean, seed: Int): util.List[Array[AnyRef]] = {
    val dataSet:DataSet[Array[Object]] = ddf.getRepresentationHandler.get(RepresentationHandler.DATASET_ARR_OBJECT.getTypeSpecsString).asInstanceOf[DataSet[Array[Object]]]
    val count = dataSet.count
    val fraction = Samples.computeFractionForSampleSize(numSamples, count,withReplacement)
    seqAsJavaList(Samples.randomSample(dataSet,withReplacement,fraction,seed).collect())
  }

  override def getRandomSample(percent: Double, withReplacement: Boolean, seed: Int): DDF = {
    if (percent > 1 || percent < 0) {
      throw new IllegalArgumentException("Sampling fraction must be from 0 to 1")
    } else {
      val dataSet: DataSet[Array[Object]] = ddf.getRepresentationHandler.get(RepresentationHandler.DATASET_ARR_OBJECT.getTypeSpecsString).asInstanceOf[DataSet[Array[Object]]]
      val newDS: DataSet[Array[Object]] = Samples.randomSample(dataSet, withReplacement, percent, seed)
      val manager = ddf.getManager
      val columns = ddf.getSchema.getColumns
      val tableName: String = ddf.getSchemaHandler.newTableName()
      val schema = new Schema(tableName, columns)
      val typeSpecs: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Array[Object]])
      val sampleDDF = manager.newDDF(manager, newDS, typeSpecs, manager.getNamespace, tableName, schema)
      mLog.info(">>>>>>> adding ddf to DDFManager " + sampleDDF.getName)
      manager.addDDF(sampleDDF)
      sampleDDF.getMetaDataHandler.copyFactor(this.getDDF)
      sampleDDF
    }
  }

  override def subset(columnExpr: util.List[Column], filter: Expression): DDF = {
    filter match {
      case op:Operator if (op.getName == OperationName.grep || op.getName == OperationName.grep_ic) =>
          throw new UnsupportedOperationException("Grep and Grep_IC are not supported in Flink DDF")
      case _=>
        super.subset(columnExpr, filter)
    }
  }

}
