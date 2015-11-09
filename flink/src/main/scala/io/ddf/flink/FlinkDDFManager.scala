package io.ddf.flink

import java.security.SecureRandom
import java.util.UUID

import io.ddf.DDFManager.EngineType
import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.exception.DDFException
import io.ddf.flink.FlinkConstants._
import io.ddf.flink.content.{Column2RowTypeInfo, RowParser}
import io.ddf.flink.utils.{DemoSupport, RowCacheHelper, Utils}
import io.ddf.misc.Config
import io.ddf.{DDF, DDFManager}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.api.table.Row
import org.slf4j.LoggerFactory

class FlinkDDFManager extends DDFManager {

  private val flinkExecutionEnvironment: ExecutionEnvironment = createExecutionEnvironment

  private final val logger = LoggerFactory.getLogger(getClass)

  this.setEngineType(EngineType.FLINK)

  override def getEngine: String = ENGINE_NAME

  override def getNamespace: String = NAMESPACE

  override def loadTable(fileURL: String, fieldSeparator: String): DDF = {

    val fileData: DataSet[String] = flinkExecutionEnvironment.readTextFile(fileURL)

    // scalastyle:off magic.number
    val sampleSize = 5
    // scalastyle:on magic.number

    val subset = fileData.first(sampleSize).collect()
    val columns: Array[Column] = getColumnInfo(subset,fieldSeparator)
    implicit val rowTypeInfo = Column2RowTypeInfo.getRowTypeInfo (columns)

    val typeSpecs: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Row])
    val rand: SecureRandom = new SecureRandom
    val tableName: String = "tbl" + String.valueOf(Math.abs(rand.nextLong))

    val schema: Schema = new Schema(tableName, columns)
    val parser = RowParser.parser(columns, useDefaults = false)
    val rdata: DataSet[Row] = fileData.map(_.split(fieldSeparator)).map(r => parser(r))

    val data: DataSet[Row] = if (DemoSupport.isDemoMode) {
      RowCacheHelper.reloadRowsFromCache(flinkExecutionEnvironment, fileURL, rowTypeInfo, rdata)
    } else {
      rdata
    }

    val ddf = this.newDDF(data, typeSpecs,tableName, schema)
    this.addDDF(ddf)
    ddf
  }

  def getExecutionEnvironment: ExecutionEnvironment = flinkExecutionEnvironment

  def getColumnInfo(sampleData: Seq[String],
                    fieldSeparator:String,
                    hasHeader: Boolean = false,
                    doPreferDouble: Boolean = true): Array[Schema.Column] = {

    val sampleSize: Int = sampleData.length
    mLog.info("Sample size: " + sampleSize)

    //    if (sampleSize < 1) {
    //      mLog.info("DATATYPE_SAMPLE_SIZE must be bigger than 1")
    //      return null
    //    }

    val firstRow: Array[String] = sampleData.head.split(fieldSeparator)

    val headers: Seq[String] = if (hasHeader) {
      firstRow.toSeq
    } else {
      val size: Int = firstRow.length
      (1 to size) map (i => s"V$i")
    }

    val sampleStrings = if (hasHeader) sampleData.tail else sampleData

    val samples = sampleStrings.map(_.split(fieldSeparator)).toArray.transpose

    samples.zipWithIndex.map {
      case (col, i) => new Schema.Column(headers(i), Utils.determineType(col, doPreferDouble, false))
    }
  }

  private def createExecutionEnvironment: ExecutionEnvironment = {
    val isLocal = java.lang.Boolean.parseBoolean(Config.getValue(ENGINE_NAME, "local"))
    val isSysoutLoggingEnabled = java.lang.Boolean.parseBoolean(Config.getValue(ENGINE_NAME, "enableSysoutLogging"))
    val executionEnvironment: ExecutionEnvironment = if (isLocal) {
      ExecutionEnvironment.getExecutionEnvironment
    } else {
      val host = Config.getValue(ENGINE_NAME, "host")
      val port = java.lang.Integer.parseInt(Config.getValue(ENGINE_NAME, "port"))
      val parallelism = java.lang.Integer.parseInt(Config.getValue(ENGINE_NAME, "parallelism"))
      ExecutionEnvironment.createRemoteEnvironment(host, port, parallelism)
    }
    if (!isSysoutLoggingEnabled) {
      executionEnvironment.getConfig.disableSysoutLogging()
    }
    executionEnvironment
  }


  override def setDDFName(ddf: DDF, name: String): Unit = {
    super.setDDFName(ddf, name)
    ddf.getSchema.setTableName(name)
  }
}

object FlinkConstants {
  val ENGINE_NAME: String = "flink"
  val NAMESPACE: String = "adatao"
}
