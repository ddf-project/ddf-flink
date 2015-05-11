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

import io.ddf.content.Schema;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.AbstractID;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: satya
 */
public class Utils {


    /**
     * Convenience method to get the elements of a DataSet as a List
     * As DataSet can contain a lot of data, this method should be used with caution.
     *
     * @return A List containing the elements of the DataSet
     */
    public static List collect(ExecutionEnvironment env, DataSet dataSet) throws Exception {
        final String id = new AbstractID().toString();
        final TypeSerializer serializer = dataSet.getType().createSerializer();

        dataSet.flatMap(new CollectHelper(id, serializer)).output(new DiscardingOutputFormat());
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


    /**
     * TODO: check more than a few lines in case some lines have NA
     *
     * @param dataSet
     * @return
     */
    public static Tuple3<String[], List<Schema.Column>, String[]> getMetaInfo(ExecutionEnvironment env, final Logger mLog, DataSet<String> dataSet, String fieldSeparator, boolean hasHeader, boolean doPreferDouble) throws Exception {
        String[] headers;
        int sampleSize = 5;


        DataSet<String> sampleData = dataSet.first(sampleSize);
        List<String> sampleStr = Utils.collect(env, sampleData);
        // actual sample size
        sampleSize = sampleStr.size();
        mLog.info("Sample size: " + sampleSize);
        // sanity check
        if (sampleSize < 1) {
            mLog.info("DATATYPE_SAMPLE_SIZE must be bigger than 1");
            return null;
        }

        // create sample list for getting data type
        String[] firstSplit = sampleStr.get(0).split(fieldSeparator);

        // get header
        if (hasHeader) {
            headers = firstSplit;
        } else {
            headers = new String[firstSplit.length];
            int size = headers.length;
            for (int i = 0; i < size; ) {
                headers[i] = "V" + (++i);
            }
        }

        String[][] samples = hasHeader ? (new String[firstSplit.length][sampleSize - 1])
                : (new String[firstSplit.length][sampleSize]);

        String[] metaInfoArray = new String[firstSplit.length];
        List<Schema.Column> columns = new ArrayList<>(firstSplit.length);
        String[] colNames = new String[firstSplit.length];
        int start = hasHeader ? 1 : 0;
        for (int j = start; j < sampleSize; j++) {
            firstSplit = sampleStr.get(j).split(fieldSeparator);
            for (int i = 0; i < firstSplit.length; i++) {
                samples[i][j - start] = firstSplit[i];
            }
        }


        for (int i = 0; i < samples.length; i++) {
            String[] vector = samples[i];
            colNames[i] = headers[i];
            metaInfoArray[i] = headers[i] + ":" + determineType(vector, doPreferDouble, false);
            Schema.Column column = new Schema.Column(headers[i], determineType(vector, doPreferDouble, true));
            columns.add(column);
        }

        return new Tuple3<>(metaInfoArray, columns, colNames);
    }

    /**
     * Given a String[] vector of data values along one column, try to infer what the data type should be.
     * <p/>
     * TODO: precompile regex
     *
     * @param vector
     * @return string representing name of the type "integer", "double", "character", or "logical" The algorithm will
     * first scan the vector to detect whether the vector contains only digits, ',' and '.', <br>
     * if true, then it will detect whether the vector contains '.', <br>
     * &nbsp; &nbsp; if true then the vector is double else it is integer <br>
     * if false, then it will detect whether the vector contains only 'T' and 'F' <br>
     * &nbsp; &nbsp; if true then the vector is logical, otherwise it is characters
     */
    public static String determineType(String[] vector, Boolean doPreferDouble, boolean isForSchema) {
        boolean isNumber = true;
        boolean isInteger = true;
        boolean isLogical = true;
        boolean allNA = true;

        for (String s : vector) {
            if (s == null || s.startsWith("NA") || s.startsWith("Na") || s.matches("^\\s*$")) {
                // Ignore, don't set the type based on this
                continue;
            }

            allNA = false;

            if (isNumber) {
                // match numbers: 123,456.123 123 123,456 456.123 .123
                if (!s.matches("(^|^-)((\\d+(,\\d+)*)|(\\d*))\\.?\\d+$")) {
                    isNumber = false;
                }
                // match double
                else if (isInteger && s.matches("(^|^-)\\d*\\.{1}\\d+$")) {
                    isInteger = false;
                }
            }

            // NOTE: cannot use "else" because isNumber changed in the previous
            // if block
            if (isLogical && !s.toLowerCase().matches("^t|f|true|false$")) {
                isLogical = false;
            }
        }

        // String result = "Unknown";
        String result = "string";

        if (!allNA) {
            if (isNumber) {
                if (isInteger) {
                    result = "int";
                } else if (doPreferDouble) {
                    result = "double";
                } else {
                    result = "float";
                }
            } else {
                if (isLogical) {
                    result = isForSchema ? "boolean" : "bool";
                } else {
                    result = "string";
                }
            }
        }
        return result;
    }

}
