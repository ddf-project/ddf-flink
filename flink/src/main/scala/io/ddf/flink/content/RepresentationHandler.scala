package io.ddf.flink.content

import java.text.SimpleDateFormat
import java.util.Date

import io.ddf.DDF
import io.ddf.content.Schema.{Column, ColumnType}
import io.ddf.content.{Representation, RepresentationHandler => RH}
import io.ddf.flink.content.RepresentationHandler._
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.{Row, Table}
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.{Vector => FVector}

import scala.util.{Failure, Success, Try}

class RepresentationHandler(ddf: DDF) extends RH(ddf) {
  override def getDefaultDataType: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Array[Object]])

  //In the current implementation of ArrayObject2Row, NA,null and invalid field values are translated to default values.
  //Use DataSet[Array[Object]] if this information is required.
  this.addConvertFunction(DATASET_ARR_OBJECT, DATASET_ROW, new ArrayObject2Row(this.ddf))
  this.addConvertFunction(DATASET_ROW, TABLE, new DataSetRow2Table(this.ddf))
  this.addConvertFunction(DATASET_ROW, DATASET_ARR_OBJECT, new Row2ArrayObject(this.ddf))
  this.addConvertFunction(TABLE, DATASET_ROW, new Table2DataSetRow(this.ddf))
  this.addConvertFunction(DATASET_ARR_OBJECT, DATASET_RList, new ArrayObject2FlinkRList(this.ddf))
  this.addConvertFunction(DATASET_ARR_OBJECT, DATASET_ARR_DOUBLE, new ArrayObject2ArrayDouble(this.ddf))
  this.addConvertFunction(DATASET_ARR_DOUBLE, DATASET_ARR_OBJECT, new ArrayDouble2ArrayObject(this.ddf))

  //ML Related representations
  this.addConvertFunction(DATASET_ARR_DOUBLE, DATASET_LABELED_VECTOR, new ArrayDouble2LabeledVector(this.ddf))
  this.addConvertFunction(DATASET_ARR_DOUBLE, DATASET_VECTOR, new ArrayDouble2Vector(this.ddf))
  this.addConvertFunction(DATASET_LABELED_VECTOR, DATASET_ARR_DOUBLE, new LabeledVector2ArrayDouble(this.ddf))
  this.addConvertFunction(DATASET_ARR_OBJECT, DATASET_TUPLE2, new ArrayObject2Tuple2(this.ddf))
  this.addConvertFunction(DATASET_ARR_OBJECT, DATASET_TUPLE3, new ArrayObject2Tuple3(this.ddf))
  this.addConvertFunction(DATASET_TUPLE2, DATASET_ARR_OBJECT, new Tuple2ToArrayObject(this.ddf))
  this.addConvertFunction(DATASET_TUPLE3, DATASET_ARR_OBJECT, new Tuple3ToArrayObject(this.ddf))
}

object RepresentationHandler {
  /**
   * Supported Representations
   */
  val DATASET_ARR_OBJECT = new Representation(classOf[DataSet[_]], classOf[Array[Object]])
  val DATASET_ROW = new Representation(classOf[DataSet[_]], classOf[Row])
  val TABLE = new Representation(classOf[Table])
  val DATASET_RList = new Representation(classOf[DataSet[_]], classOf[FlinkRList])

  val DATASET_ROW_TYPE_SPECS = Array(classOf[DataSet[_]], classOf[Row])
  val TABLE_TYPE_SPECS = Array(classOf[Table])
  val DATASET_ARR_OBJ_TYPE_SPECS = Array(classOf[DataSet[_]], classOf[Array[Object]])
  val DATASET_ARR_DOUBLE = new Representation(classOf[DataSet[_]], classOf[Array[Double]])

  val DATASET_LABELED_VECTOR = new Representation(classOf[DataSet[_]], classOf[LabeledVector])
  val DATASET_VECTOR = new Representation(classOf[DataSet[_]], classOf[FVector])
  val DATASET_TUPLE2 = new Representation(classOf[DataSet[_]], classOf[Tuple2[_, _ ]], classOf[Int], classOf[Int])
  val DATASET_TUPLE3 = new Representation(classOf[DataSet[_]], classOf[Tuple3[_,_,_]], classOf[Int], classOf[Int], classOf[Double])

  def getRowDataSet(dataSet: DataSet[_], columns: List[Column]): DataSet[Row] = {
    val idxColumns: Seq[(Column, Int)] = columns.zipWithIndex.toSeq
    implicit val rowTypeInfo = Column2RowTypeInfo.getRowTypeInfo(columns)
    //val rowDataSet = dataSet.asInstanceOf[DataSet[Array[Object]]].map(r => parseRow(r, idxColumns, useDefaults))
    val rowDataSet = dataSet.asInstanceOf[DataSet[Array[Any]]].map(a => new Row(a))
    rowDataSet
  }
}

object RowParser extends Serializable {

  private val dateFormat = new SimpleDateFormat()

  def parseRow(rowArray: Array[Object], idxColumns: Seq[(Column, Int)], useDefaults: Boolean): Row = {
    val row = new Row(idxColumns.length)
    idxColumns foreach {
      case (col, idx) =>
        val colValue: String = getFieldValue(rowArray(idx), col.isNumeric)
        col.getType match {
          case ColumnType.STRING =>
            row.setField(idx, colValue)
          case ColumnType.INT =>
            row.setField(idx, Try(colValue.toInt).getOrElse(if (useDefaults) 0 else null))
          case ColumnType.FLOAT =>
            row.setField(idx, Try(colValue.toFloat).getOrElse(if (useDefaults) 0 else null))
          case ColumnType.DOUBLE =>
            row.setField(idx, Try(colValue.toDouble).getOrElse(if (useDefaults) 0 else null))
          case ColumnType.BIGINT =>
            row.setField(idx, Try(colValue.toDouble).getOrElse(if (useDefaults) 0 else null))
          case ColumnType.TIMESTAMP =>
            row.setField(idx, Try(dateFormat.parse(colValue)).getOrElse(if (useDefaults) new Date(0) else null))
          case ColumnType.BOOLEAN =>
            row.setField(idx, Try(colValue.toBoolean).getOrElse(if (useDefaults) false else null))
        }
    }
    row
  }


  private def getFieldValue(elem: Object, isNumeric: Boolean): String = {
    val mayBeString: Try[String] = Try(elem.toString.trim)
    mayBeString match {
      case Success(s) if isNumeric && s.equalsIgnoreCase("NA") => null
      case Success(s) => s
      case Failure(e) => null
    }
  }
}

