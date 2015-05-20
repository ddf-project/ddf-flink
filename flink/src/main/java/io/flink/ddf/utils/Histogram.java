package io.flink.ddf.utils;


import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class Histogram implements Accumulator<Double, Map<Double, Integer>> {
    private static final long serialVersionUID = 1L;
    private TreeMap<Double, Integer> treeMap = new TreeMap();

    public Histogram() {
    }

    public Histogram(Double[] binKeys) {
        for (Double key : binKeys) {
            treeMap.put(key, null);
        }
    }

    public void add(Double value) {
        Double theKey = this.treeMap.floorKey(value);
        theKey = theKey == null ? value : theKey;
        Integer current = this.treeMap.get(theKey);
        Integer newValue = Integer.valueOf((current != null ? current.intValue() : 0) + 1);
        this.treeMap.put(theKey, newValue);
    }

    public Map<Double, Integer> getLocalValue() {
        return this.treeMap;
    }

    public void merge(Accumulator<Double, Map<Double, Integer>> other) {
        Iterator<Map.Entry<Double, Integer>> i$ = ((Histogram) other).getLocalValue().entrySet().iterator();

        while (i$.hasNext()) {
            Map.Entry<Double, Integer> entryFromOther = (Map.Entry) i$.next();
            Integer ownValue = this.treeMap.get(entryFromOther.getKey());
            if (ownValue == null) {
                this.treeMap.put(entryFromOther.getKey(), entryFromOther.getValue());
            } else {
                this.treeMap.put(entryFromOther.getKey(), Integer.valueOf(((Integer) entryFromOther.getValue()).intValue() + ownValue.intValue()));
            }
        }

    }

    public void resetLocal() {
        this.treeMap.clear();
    }

    public String toString() {
        return this.treeMap.toString();
    }

    public void write(DataOutputView out) throws IOException {
        out.writeInt(this.treeMap.size());
        Iterator i$ = this.treeMap.entrySet().iterator();

        while (i$.hasNext()) {
            Map.Entry<Double, Integer> entry = (Map.Entry) i$.next();
            out.writeDouble(entry.getKey());
            int val = entry.getValue() != null ? entry.getValue() : 0;
            out.writeInt(val);
        }

    }

    public void read(DataInputView in) throws IOException {
        int size = in.readInt();

        for (int i = 0; i < size; ++i) {
            this.treeMap.put(Double.valueOf(in.readDouble()), Integer.valueOf(in.readInt()));
        }

    }
}
