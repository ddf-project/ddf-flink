/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.ddf.flink.content

import io.ddf.DDF
import io.ddf.content.{ConvertFunction, Representation}
import io.ddf.exception.DDFException
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.{Vector => FVector}
import org.apache.flink.api.scala._



class LabeledVectorToTuple2Vector(@transient ddf: DDF) extends ConvertFunction(ddf) {

  override def apply(representation: Representation): Representation = {
    val numCols = ddf.getNumColumns
    representation.getValue match {
      case dataset: DataSet[LabeledVector] => {
        val datasetVectorTuple: DataSet[(FVector, Double)] = dataset.map {
          lv => {
            (lv.vector, lv.label)
          }
        }
        new Representation(datasetVectorTuple, RepresentationHandler.DATASET_TUPLE2_VECTOR.getTypeSpecsString)
      }
      case _ => throw new DDFException("Error getting RDD[LabeledVector]")
    }
  }
}
