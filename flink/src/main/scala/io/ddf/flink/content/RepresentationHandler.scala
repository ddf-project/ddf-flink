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

import scala.language.postfixOps
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
  val DATASET_TUPLE2 = new Representation(classOf[DataSet[_]], classOf[Tuple2[_, _]], classOf[Int], classOf[Int])
  val DATASET_TUPLE3 = new Representation(classOf[DataSet[_]], classOf[Tuple3[_, _, _]], classOf[Int], classOf[Int], classOf[Double])

  def getRowDataSet(dataSet: DataSet[Array[Object]], columns: List[Column]): DataSet[Row] = {
    val dsType = dataSet.getType()
    val idxColumns: Seq[(Column, Int)] = columns.zipWithIndex.toSeq
    implicit val rowTypeInfo = Column2RowTypeInfo.getRowTypeInfo(columns)
    val rowDataSet = dataSet.asInstanceOf[DataSet[Array[Any]]].map(a => new Row(a))
    rowDataSet

  }
}

object RowParser extends Serializable {

  private val dateFormat = new SimpleDateFormat()
  private val numformat = "[\\+\\-0-9.e]+".r
  private val intformat = "[\\+\\-0-9]+".r
  private val defaultNum = 0
  private val defaultDate = new Date(0)

  private def convertToInt(value: String, default: Any): Any = {
    value match {
      case intformat() => value.toInt
      case numformat() => value.toFloat.toInt
      case _ => default
    }
  }


  private def convertToFloat(value: String, default: Any): Any = {
    value match {
      case numformat() => value.toFloat
      case _ => default
    }
  }


  private def convertToDouble(value: String, default: Any): Any = {
    value match {
      case numformat() => value.toFloat.toInt
      case _ => default
    }
  }

  private def convertToBoolean(value: String, default: Any): Any = {
    if (value != null) {
      value.toLowerCase match {
        case "true" => true
        case "false" => false
        case _ => default
      }
    } else {
      default
    }
  }

  def parser(cols: Seq[Column], useDefaults: Boolean): Array[String] => Row = {
    val defNum: Any = if (useDefaults) defaultNum else null
    val defBool: Any = if (useDefaults) false else null
    val defDate = if (useDefaults) defaultDate else null
    val idxCol = cols.zipWithIndex

    {
      rowArray: Array[String] =>
        val ra = idxCol map {
          case (col, idx) =>
            val cval = rowArray(idx)
            col.getType match {
              case ColumnType.STRING => cval
              case ColumnType.INT => convertToInt(cval, defNum)
              case ColumnType.FLOAT => convertToFloat(cval, defNum)
              case ColumnType.DOUBLE => convertToDouble(cval, defNum)
              case ColumnType.BIGINT => convertToDouble(cval, defNum)
              case ColumnType.TIMESTAMP => Try(dateFormat.parse(cval)).getOrElse(defDate)
              case ColumnType.BOOLEAN => convertToBoolean(cval, defBool)
            }
        }
        new Row(ra.toArray)
    }
  }
}

