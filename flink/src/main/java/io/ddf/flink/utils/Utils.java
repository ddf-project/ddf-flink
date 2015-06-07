package io.ddf.flink.utils;

import io.ddf.content.Schema;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.AbstractID;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Utils {

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
        List<String> sampleStr = sampleData.collect();
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
