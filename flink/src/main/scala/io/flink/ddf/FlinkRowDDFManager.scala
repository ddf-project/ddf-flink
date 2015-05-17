package io.flink.ddf

import java.io.IOException
import java.security.SecureRandom
import java.util

import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.{DDF, DDFManager}
import io.flink.ddf.utils.{CollectHelper, SerializedListAccumulator, Utils}
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.api.table.Row
import org.apache.flink.runtime.AbstractID
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class FlinkRowDDFManager extends DDFManager {

  private var flinkExecutionEnvironment: ExecutionEnvironment = ExecutionEnvironment.createLocalEnvironment()

  private final val logger = LoggerFactory.getLogger(getClass)

  override def getEngine: String = "flink-row"

  override def loadTable(fileURL: String, fieldSeparator: String): DDF = {
    val fileData = flinkExecutionEnvironment.readTextFile(fileURL)

    val columns: Array[Column] = getColumnInfo(fileData.first(5), fieldSeparator)

    val colSize = columns.length

    val data: DataSet[Row] = fileData.map {
      line =>
        val row = new Row(colSize)
        val fields = line.split(fieldSeparator)
        (0 to colSize-1).foreach { index =>
          row.setField(index, fields(index))
        }
        row
    }

    val typeSpecs: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Row])
    val namespace: String = "flinkRowDDF"
    val rand: SecureRandom = new SecureRandom
    val tableName: String = "tbl" + String.valueOf(Math.abs(rand.nextLong))

    val schema: Schema = new Schema(tableName, columns)

    val ddf = this.newDDF(data, typeSpecs, namespace, tableName, schema)
    ddf
  }

  def getExecutionEnvironment = flinkExecutionEnvironment

  /**
   * Convenience method to get the elements of a DataSet as a List
   * As DataSet can contain a lot of data, this method should be used with caution.
   *
   * @return A List containing the elements of the DataSet
   */
  @throws(classOf[Exception])
  def collect[T: TypeInformation : ClassTag](env: ExecutionEnvironment, dataSet: DataSet[T]): List[T] = {
    val id: String = new AbstractID().toString
    val serializer: TypeSerializer[T] = dataSet.getType.createSerializer
    dataSet.flatMap(new CollectHelper[T](id, serializer)).output(new DiscardingOutputFormat[T])
    val res: JobExecutionResult = env.execute
    val accResult: util.ArrayList[Array[Byte]] = res.getAccumulatorResult(id)
    try {
      SerializedListAccumulator.deserializeList(accResult, serializer).asScala.toList
    }
    catch {
      case e: ClassNotFoundException => {
        throw new RuntimeException("Cannot find type class of collected data type.", e)
      }
      case e: IOException => {
        throw new RuntimeException("Serialization error while de-serializing collected data", e)
      }
    }
  }

  def getColumnInfo(dataSet: DataSet[String],
                    fieldSeparator: String,
                    hasHeader: Boolean = false,
                    doPreferDouble: Boolean = true): Array[Schema.Column] = {

    var sampleSize: Int = 5
    val sampleData: DataSet[String] = dataSet.first(sampleSize)
    val sampleStr: List[String] = collect(flinkExecutionEnvironment, sampleData)
    sampleSize = sampleStr.size
    mLog.info("Sample size: " + sampleSize)

    //    if (sampleSize < 1) {
    //      mLog.info("DATATYPE_SAMPLE_SIZE must be bigger than 1")
    //      return null
    //    }

    val firstSplit: Array[String] = sampleStr.head.split(fieldSeparator)

    val headers: Seq[String] = if (hasHeader) {
      firstSplit.toSeq
    } else {
      val size: Int = firstSplit.length
      (1 to size) map (i => s"V$i")
    }

    val sampleStrings = if (hasHeader) sampleStr.tail else sampleStr

    val samples = sampleStrings.map(_.split(fieldSeparator)).toArray.transpose

    samples.zipWithIndex.map {
      case (col, i) => new Schema.Column(headers(i), Utils.determineType(col, doPreferDouble, false))
    }
  }
}
