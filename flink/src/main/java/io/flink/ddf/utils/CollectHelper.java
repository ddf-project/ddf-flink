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

package io.flink.ddf.utils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * User: satya
 */
public class CollectHelper<T> extends RichFlatMapFunction<T, T> {

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
