package io.flink.ddf.utils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

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
