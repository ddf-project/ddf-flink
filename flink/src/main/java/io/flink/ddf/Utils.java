/*
 * Copyright 2014, Tuplejump Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.flink.ddf;

import io.flink.ddf.utils.SerializedListAccumulator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.AbstractID;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: satya
 */
public class Utils {

    public static class CollectHelper<T> extends RichFlatMapFunction<T, T> {

        private static final long serialVersionUID = 1L;

        private final String id;
        private final TypeSerializer<T> serializer;

        private SerializedListAccumulator<T> accumulator;

        public CollectHelper(String id, TypeSerializer<T> serializer) {
            this.id = id;
            this.serializer = serializer;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.accumulator = new SerializedListAccumulator<T>();
            getRuntimeContext().addAccumulator(id, accumulator);
        }

        @Override
        public void flatMap(T value, Collector<T> out) throws Exception {
            accumulator.add(value, serializer);
        }
    }


    /**
     * Convenience method to get the elements of a DataSet as a List
     * As DataSet can contain a lot of data, this method should be used with caution.
     *
     * @return A List containing the elements of the DataSet
     */
    public static List collect(ExecutionEnvironment env, DataSet dataSet) throws Exception {
        final String id = new AbstractID().toString();
        final TypeSerializer serializer = dataSet.getType().createSerializer();

        dataSet.flatMap(new Utils.CollectHelper(id, serializer)).output(new DiscardingOutputFormat());
        JobExecutionResult res = env.execute();

        ArrayList<byte[]> accResult = res.getAccumulatorResult(id);
        try {
            return SerializedListAccumulator.deserializeList(accResult, serializer);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Cannot find type class of collected data type.", e);
        } catch (IOException e) {
            throw new RuntimeException("Serialization error while de-serializing collected data", e);
        }
    }

}
