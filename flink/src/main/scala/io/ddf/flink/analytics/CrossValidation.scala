package io.ddf.flink.analytics

import java.util
import java.util.{List => JList, Random}

import io.ddf.content.Schema
import io.ddf.exception.DDFException
import io.ddf.flink.utils.Misc.isNull
import io.ddf.ml.CrossValidationSet
import io.ddf.{DDF, DDFManager}
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.Row

import scala.reflect.ClassTag

class RandomFilterFunction[T](seed: Long,
                              lower: Double,
                              upper: Double,
                              isTraining: Boolean) extends RichFilterFunction[T] {
  val rand = new Random(seed)

  override def filter(t: T): Boolean = {
    if (isTraining) {
      val z = rand.nextDouble
      z < lower || z >= upper
    } else {
      val z = rand.nextDouble
      z < lower || z >= upper
    }
  }
}

object CrossValidation {
  /** Return an Iterator of size k of (train, test) RDD Tuple
    * for which the probability of each element belonging to each split is (trainingSize, 1-trainingSize).
    * The train & test data across k split are shuffled differently (different random seed for each iteration).
    */
  def randomSplit[T](dataset: DataSet[T],
                     numSplits: Int,
                     trainingSize: Double,
                     seed: Long)(
                      implicit _cm: ClassTag[T]): Iterator[(DataSet[T], DataSet[T])] = {
    require(0 < trainingSize && trainingSize < 1)
    val rg = new Random(seed)
    (1 to numSplits).map(_ => rg.nextInt).map(z =>
      (dataset.filter(new RandomFilterFunction[T](z, 0, 1.0 - trainingSize, true)),
        dataset.filter(new RandomFilterFunction[T](z, 0, 1.0 - trainingSize, false)))).toIterator
  }

  /** Return an Iterator of of size k of (train, test) RDD Tuple
    * for which the probability of each element belonging to either split is ((k-1)/k, 1/k).
    * The location of the test data is shifted consistently between folds
    * so that the resulting test sets are pair-wise disjoint.
    */
  def kFoldSplit[T](dataset: DataSet[T],
                    numSplits: Int,
                    seed: Long)(
                     implicit _cm: ClassTag[T]): Iterator[(DataSet[T], DataSet[T])] = {
    require(numSplits > 0)
    (for (lower <- 0.0 until 1.0 by 1.0 / numSplits)
      yield (dataset.filter(new RandomFilterFunction[T](seed, lower, lower + 1.0 / numSplits, true)),
        dataset.filter(new RandomFilterFunction[T](seed, lower, lower + 1.0 / numSplits, false)))
      ).toIterator
  }

  def DDFRandomSplit(ddf: DDF,
                     numSplits: Int,
                     trainingSize: Double,
                     seed: Long): JList[CrossValidationSet] = {
    var unitType: Class[_] = null
    var splits: Iterator[(DataSet[_], DataSet[_])] = null
    if (ddf.getRepresentationHandler.has(classOf[DataSet[_]], classOf[Row])) {
      val rdd = ddf.getRepresentationHandler().get(classOf[DataSet[_]], classOf[Row]).asInstanceOf[DataSet[Row]]
      splits = randomSplit(rdd, numSplits, trainingSize, seed)
      unitType = classOf[Row]

    } else if (ddf.getRepresentationHandler.has(classOf[DataSet[_]], classOf[Array[Double]])) {
      val rdd = ddf.getRepresentationHandler().get(classOf[DataSet[_]], classOf[Array[Double]]).asInstanceOf[DataSet[Array[Double]]]
      splits = randomSplit(rdd, numSplits, trainingSize, seed)
      unitType = classOf[Array[Double]]

    } else if (ddf.getRepresentationHandler.has(classOf[DataSet[_]], classOf[Array[Object]])) {
      val rdd = ddf.getRepresentationHandler().get(classOf[DataSet[_]], classOf[Array[Object]]).asInstanceOf[DataSet[Array[Object]]]
      splits = randomSplit(rdd, numSplits, trainingSize, seed)
      unitType = classOf[Array[Object]]

    } else {
      val rdd = ddf.getRepresentationHandler().get(classOf[DataSet[_]], classOf[Array[Object]]).asInstanceOf[DataSet[Array[Object]]]
      if (isNull(rdd)) {
        throw new DDFException("Cannot get RDD of Representation Array[Double], Array[Object] or Row")
      }
      splits = randomSplit(rdd, numSplits, trainingSize, seed)
      unitType = classOf[Array[Object]]
    }
    if (isNull(splits)) {
      throw new DDFException("Error getting cross validation for DDF")
    }
    getDDFCVSetsFromDataSets(splits, ddf.getManager, ddf.getSchema, ddf.getNamespace, unitType)
  }

  def DDFKFoldSplit(ddf: DDF, numSplits: Int, seed: Long): JList[CrossValidationSet] = {
    var unitType: Class[_] = null
    var splits: Iterator[(DataSet[_], DataSet[_])] = null

    if (ddf.getRepresentationHandler.has(classOf[DataSet[_]], classOf[Row])) {
      val rdd = ddf.getRepresentationHandler().get(classOf[DataSet[_]], classOf[Row]).asInstanceOf[DataSet[Row]]
      splits = kFoldSplit(rdd, numSplits, seed)
      unitType = classOf[Row]

    } else if (ddf.getRepresentationHandler.has(Array(classOf[DataSet[_]], classOf[Array[Double]]): _*)) {
      val rdd = ddf.getRepresentationHandler().get(classOf[DataSet[_]], classOf[Array[Double]]).asInstanceOf[DataSet[Array[Double]]]
      splits = kFoldSplit(rdd, numSplits, seed)
      unitType = classOf[Array[Double]]

    } else if (ddf.getRepresentationHandler.has(Array(classOf[DataSet[_]], classOf[Array[Object]]): _*)) {
      val rdd = ddf.getRepresentationHandler().get(classOf[DataSet[_]], classOf[Array[Object]]).asInstanceOf[DataSet[Array[Object]]]
      splits = kFoldSplit(rdd, numSplits, seed)
      unitType = classOf[Array[Object]]

    } else {
      val rdd = ddf.getRepresentationHandler().get(classOf[DataSet[_]], classOf[Array[Object]]).asInstanceOf[DataSet[Array[Object]]]
      if (isNull(rdd)) throw new DDFException("Cannot get RDD of representation Array[Double], Array[Object] or Row")
      splits = kFoldSplit(rdd, numSplits, seed)
      unitType = classOf[Array[Object]]
    }

    if (isNull(splits)) {
      throw new DDFException("Error getting cross validation for DDF")
    }
    getDDFCVSetsFromDataSets(splits, ddf.getManager, ddf.getSchema, ddf.getNamespace, unitType)
  }

  /**
   * Get set of Cross Validation of DDFs from RDD splits
   * @param splits Iterator of tuple of (train, test) RDD
   * @param unitType unitType of returned DDF
   * @return List of Cross Validation sets
   */
  private def getDDFCVSetsFromDataSets(splits: Iterator[(DataSet[_], DataSet[_])],
                                       manager: DDFManager,
                                       schema: Schema,
                                       nameSpace: String,
                                       unitType: Class[_]):
  JList[CrossValidationSet] = {
    // val cvSets: JList[JList[DDF]] = new util.ArrayList[JList[DDF]]()
    val cvSets: JList[CrossValidationSet] = new util.ArrayList[CrossValidationSet]()

    for ((train, test) <- splits) {
      // val aSet = new util.ArrayList[DDF]()

      val trainSchema = new Schema(null, schema.getColumns)

      val typeSpecs: Array[Class[_]] = Array(classOf[DataSet[_]], unitType)
      val trainDDF = manager.newDDF(manager, train, typeSpecs, nameSpace, trainSchema.getTableName, trainSchema)

      val testSchema = new Schema(null, schema.getColumns)
      val testDDF = manager.newDDF(manager, test, typeSpecs, nameSpace, testSchema.getTableName, testSchema)

      cvSets.add(new CrossValidationSet(trainDDF, testDDF))
    }
    cvSets
  }
}
