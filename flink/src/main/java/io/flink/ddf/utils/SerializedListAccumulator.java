package io.flink.ddf.utils;


import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * This accumulator stores a collection of objects in serialized form, so that the stored objects
 * are not affected by modifications to the original objects.
 * <p/>
 * Objects may be deserialized on demand with a specific classloader.
 *
 * @param <T> The type of the accumulated objects
 */
public class SerializedListAccumulator<T> implements Accumulator<T, ArrayList<byte[]>> {

    private static final long serialVersionUID = 1L;

    private ArrayList<byte[]> localValue = new ArrayList<byte[]>();


    @Override
    public void add(T value) {
        throw new UnsupportedOperationException();
    }

    public void add(T value, TypeSerializer<T> serializer) throws IOException {
        try {
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            OutputViewDataOutputStreamWrapper out =
                    new OutputViewDataOutputStreamWrapper(new DataOutputStream(outStream));

            serializer.serialize(value, out);
            localValue.add(outStream.toByteArray());
        } catch (IOException e) {
            throw new IOException("Failed to serialize value '" + value + '\'', e);
        }
    }

    @Override
    public ArrayList<byte[]> getLocalValue() {
        return localValue;
    }

    @Override
    public void resetLocal() {
        localValue.clear();
    }

    @Override
    public void merge(Accumulator<T, ArrayList<byte[]>> other) {
        localValue.addAll(other.getLocalValue());
    }

    @Override
    public SerializedListAccumulator<T> clone() {
        SerializedListAccumulator<T> newInstance = new SerializedListAccumulator<T>();
        newInstance.localValue = new ArrayList<byte[]>(localValue);
        return newInstance;
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> deserializeList(ArrayList<byte[]> data, TypeSerializer<T> serializer)
            throws IOException, ClassNotFoundException {
        List<T> result = new ArrayList<T>(data.size());
        for (byte[] bytes : data) {
            ByteArrayInputStream inStream = new ByteArrayInputStream(bytes);
            InputViewDataInputStreamWrapper in = new InputViewDataInputStreamWrapper(new DataInputStream(inStream));
            T val = serializer.deserialize(in);
            result.add(val);
        }
        return result;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeInt(localValue.size());
        for (byte[] el : localValue) {
            out.writeInt(el.length);
            out.write(el);
        }
    }

    @Override
    public void read(DataInputView in) throws IOException {
        int size = in.readInt();
        this.localValue = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            int numBytes = in.readInt();
            //TODO use a scratch byte buffer if possible
            byte[] bytes = new byte[numBytes];
            in.read(bytes);
            localValue.add(bytes);
        }
    }
}
