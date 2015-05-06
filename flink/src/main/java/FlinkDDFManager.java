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

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;

import java.util.ArrayList;
import java.util.List;

/**
 * User: satya
 */
public class FlinkDDFManager extends DDFManager {

    protected ExecutionEnvironment env;

    public FlinkDDFManager(ExecutionEnvironment env) {
        this.env = env;
    }

    @Override
    public DDF loadTable(String fileURL, String fieldSeparator) throws DDFException {
        DataSet<String> text = env.readTextFile(fileURL, fieldSeparator);
        return null;
    }

    @Override
    public String getEngine() {
        return "flink";
    }

    /**
     * TODO: check more than a few lines in case some lines have NA
     *
     * @param dataSet
     * @return
     */
    public String[] getMetaInfo(DataSet<String> dataSet, String fieldSeparator) {
        String[] headers = null;
        int sampleSize = 5;

        // sanity check
        if (sampleSize < 1) {
            mLog.info("DATATYPE_SAMPLE_SIZE must be bigger than 1");
            return null;
        }

        //DataSet<String> sampleStr= dataSet.first(sampleSize).re;
        //TODO
        List<String> sampleStr = new ArrayList<>();
        // actual sample size
        mLog.info("Sample size: " + sampleSize);

        // create sample list for getting data type
        String[] firstSplit = sampleStr.get(0).split(fieldSeparator);

        // get header
        boolean hasHeader = false;
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
        int start = hasHeader ? 1 : 0;
        for (int j = start; j < sampleSize; j++) {
            firstSplit = sampleStr.get(j).split(fieldSeparator);
            for (int i = 0; i < firstSplit.length; i++) {
                samples[i][j - start] = firstSplit[i];
            }
        }

        boolean doPreferDouble = true;
        for (int i = 0; i < samples.length; i++) {
            String[] vector = samples[i];
            metaInfoArray[i] = headers[i] + " " + determineType(vector, doPreferDouble);
        }

        return metaInfoArray;
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
    public static String determineType(String[] vector, Boolean doPreferDouble) {
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
                if (!isInteger || doPreferDouble) {
                    result = "double";
                } else {
                    result = "int";
                }
            } else {
                if (isLogical) {
                    result = "boolean";
                } else {
                    result = "string";
                }
            }
        }
        return result;
    }
}
