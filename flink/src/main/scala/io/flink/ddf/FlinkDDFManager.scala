package io.flink.ddf

import java.security.SecureRandom

import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.{DDF, DDFManager}
import io.flink.ddf.utils.Utils
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.slf4j.LoggerFactory

class FlinkDDFManager extends DDFManager {

  private val flinkExecutionEnvironment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  private final val logger = LoggerFactory.getLogger(getClass)

  override def getEngine: String = "flink"

  override def loadTable(fileURL: String, fieldSeparator: String): DDF = {
    val fileData: DataSet[Array[Object]] = flinkExecutionEnvironment
      .readTextFile(fileURL)
      .map(_.split(fieldSeparator).map(_.asInstanceOf[Object]))

    val subset = fileData.first(5).map(_.map(_.toString)).collect()
    val columns: Array[Column] = getColumnInfo(subset)

    val typeSpecs: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Array[Object]])
    val namespace: String = "FlinkDDF"
    val rand: SecureRandom = new SecureRandom
    val tableName: String = "tbl" + String.valueOf(Math.abs(rand.nextLong))

    val schema: Schema = new Schema(tableName, columns)

    val ddf = this.newDDF(fileData, typeSpecs, namespace, tableName, schema)
    this.addDDF(ddf)
    ddf
  }

  def getExecutionEnvironment = flinkExecutionEnvironment

  def getColumnInfo(sampleData: Seq[Array[String]],
                    hasHeader: Boolean = false,
                    doPreferDouble: Boolean = true): Array[Schema.Column] = {

    val sampleSize: Int = sampleData.length
    mLog.info("Sample size: " + sampleSize)

    //    if (sampleSize < 1) {
    //      mLog.info("DATATYPE_SAMPLE_SIZE must be bigger than 1")
    //      return null
    //    }

    val firstRow: Array[String] = sampleData.head

    val headers: Seq[String] = if (hasHeader) {
      firstRow.toSeq
    } else {
      val size: Int = firstRow.length
      (1 to size) map (i => s"V$i")
    }

    val sampleStrings = if (hasHeader) sampleData.tail else sampleData

    val samples = sampleStrings.toArray.transpose

    samples.zipWithIndex.map {
      case (col, i) => new Schema.Column(headers(i), Utils.determineType(col, doPreferDouble, false))
    }
  }
}
