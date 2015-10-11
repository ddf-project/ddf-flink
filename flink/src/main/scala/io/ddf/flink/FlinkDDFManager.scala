package io.ddf.flink

import java.security.SecureRandom
import java.util.UUID

import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.exception.DDFException
import io.ddf.flink.FlinkConstants._
import io.ddf.flink.content.RepresentationHandler
import io.ddf.flink.utils.Utils
import io.ddf.misc.Config
import io.ddf.{DDF, DDFManager}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.api.table.Row
import org.slf4j.LoggerFactory

class FlinkDDFManager extends DDFManager {

  private val flinkExecutionEnvironment: ExecutionEnvironment = createExecutionEnvironment

  private final val logger = LoggerFactory.getLogger(getClass)

  override def getEngine: String = ENGINE_NAME

  override def loadTable(fileURL: String, fieldSeparator: String): DDF = {
    val fileData: DataSet[Array[Object]] = flinkExecutionEnvironment
      .readTextFile(fileURL)
      .map(_.split(fieldSeparator).map(_.asInstanceOf[Object]))

    // scalastyle:off magic.number
    val sampleSize = 5
    // scalastyle:on magic.number

    val subset = fileData.first(sampleSize).map(_.map(_.toString)).collect()
    val columns: Array[Column] = getColumnInfo(subset)

    val typeSpecs: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Row])
    val rand: SecureRandom = new SecureRandom
    val tableName: String = "tbl" + String.valueOf(Math.abs(rand.nextLong))

    val schema: Schema = new Schema(tableName, columns)
    val rowDS = RepresentationHandler.getRowDataSet(fileData, columns.toList, useDefaults = false)

    val ddf = this.newDDF(rowDS, typeSpecs, getEngine, FlinkConstants.NAMESPACE, tableName, schema)
    this.addDDF(ddf)
    ddf
  }

  def getExecutionEnvironment: ExecutionEnvironment = flinkExecutionEnvironment

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

  private def createExecutionEnvironment: ExecutionEnvironment = {
    val isLocal = java.lang.Boolean.parseBoolean(Config.getValue(ENGINE_NAME, "local"))
    if (isLocal) {
      ExecutionEnvironment.getExecutionEnvironment
    } else {
      val host = Config.getValue(ENGINE_NAME, "host")
      val port = java.lang.Integer.parseInt(Config.getValue(ENGINE_NAME, "port"))
      val parallelism = java.lang.Integer.parseInt(Config.getValue(ENGINE_NAME, "parallelism"))
      ExecutionEnvironment.createRemoteEnvironment(host, port, parallelism)
    }
  }

  override def transfer(fromEngine: String, ddfURI: String): DDF = {
    throw new DDFException("transfer is not supported for Flink", new UnsupportedOperationException)
  }

  override def getOrRestoreDDFUri(ddfURI: String): DDF = {
    throw new DDFException("this is currently unsupported", new UnsupportedOperationException)
  }

  override def getOrRestoreDDF(uuid: UUID): DDF = {
    throw new DDFException("this is currently unsupported", new UnsupportedOperationException)
  }
}

object FlinkConstants {
  val ENGINE_NAME: String = "flink"
  val NAMESPACE: String = "FlinkDDF"
}
