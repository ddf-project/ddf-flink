package io.ddf.flink.utils;

import org.apache.flink.api.common.accumulators.Accumulator;

import java.util.Map;
import java.util.TreeMap;

/**
 * Implementation of Flink's Histogram with bins
 */
public class HistogramForDouble implements Accumulator<Double, TreeMap<Double, Integer>> {

    private static final long serialVersionUID = 1L;

    private TreeMap<Double, Integer> treeMap = new TreeMap<>();

    public HistogramForDouble() {
    }

    public HistogramForDouble(Double[] binKeys) {
        for (Double key : binKeys) {
            treeMap.put(key, null);
        }
    }

    @Override
    public void add(Double value) {
        Double theKey = this.treeMap.floorKey(value);
        theKey = theKey == null ? value : theKey;
        Integer current = this.treeMap.get(theKey);
        Integer newValue = (current != null ? current : 0) + 1;
        this.treeMap.put(theKey, newValue);

    }

    @Override
    public TreeMap<Double, Integer> getLocalValue() {
        return this.treeMap;
    }

    @Override
    public void merge(Accumulator<Double, TreeMap<Double, Integer>> other) {

        // Merge the values into this map
        for (Map.Entry<Double, Integer> entryFromOther : other.getLocalValue().entrySet()) {
            Integer ownValue = this.treeMap.get(entryFromOther.getKey());
            if (ownValue == null) {
                this.treeMap.put(entryFromOther.getKey(), entryFromOther.getValue());
            } else {
                this.treeMap.put(entryFromOther.getKey(), entryFromOther.getValue() + ownValue);
            }
        }
    }

    @Override
    public void resetLocal() {
        this.treeMap.clear();
    }

    @Override
    public String toString() {
        return this.treeMap.toString();
    }

    @Override
    public Accumulator<Double, TreeMap<Double, Integer>> clone() {
        HistogramForDouble result = new HistogramForDouble();
        result.treeMap = new TreeMap<>(treeMap);
        return result;
    }
}

