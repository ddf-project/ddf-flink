package io.flink.ddf;/*
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
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.mrql.Config;
import org.apache.mrql.Evaluator;
import org.apache.mrql.FlinkEvaluator;
import org.apache.mrql.MRQL;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

/**
 * User: satya
 */
public class FlinkDDFManager extends DDFManager {

    protected ExecutionEnvironment env;

    public FlinkDDFManager() {

        try {
            String isLocalModeStr = io.ddf.misc.Config.getValue(io.ddf.misc.Config.ConfigConstant.ENGINE_NAME_FLINK.toString(), "local");
            Config.local_mode = Boolean.parseBoolean(isLocalModeStr);
            Config.flink_mode = true;
            MRQL.clean();
            this.env = ((FlinkEvaluator) Evaluator.evaluator).flink_env;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public DDF loadTable(String fileURL, String fieldSeparator) throws DDFException {
        //TODO from Satya - Is there a better way?
        // Roundabout way of loading a FlinkDDF.
        // FlinkDDF is made by first source it via MRQL
        // then storing back as a file
        // then reading the file as a Flink DataSet and storing it as a representation
        // this is so that mutable tables do not affect the original file
        try {
            DataSet<String> text = env.readTextFile(fileURL, fieldSeparator);
            Tuple3<String[], List<Schema.Column>, String[]> metaInfo = getMetaInfo(text, fieldSeparator, false, true);
            String[] metaInfoForMRQL = metaInfo.f0;
            List<Schema.Column> metaInfoForSchema = metaInfo.f1;
            SecureRandom rand = new SecureRandom();
            String tableName = "tbl" + String.valueOf(Math.abs(rand.nextLong()));
            Schema schema = new Schema(tableName, metaInfoForSchema);
            String source = String.format("%s = source(line,'%s','%s',type(<%s>)); select (%s) from %s;",
                    tableName, fileURL, fieldSeparator, StringUtils.join(metaInfoForMRQL, ","), StringUtils.join(metaInfo.f2), tableName);
            return this.sql2ddf(source, schema);
        } catch (Exception e) {
            throw new DDFException(e);
        }
    }

    @Override
    public String getEngine() {
        return "flink";
    }

    public ExecutionEnvironment getExecutionEnvironment() {
        return env;
    }

    /**
     * TODO: check more than a few lines in case some lines have NA
     *
     * @param dataSet
     * @return
     */
    public Tuple3<String[], List<Schema.Column>, String[]> getMetaInfo(DataSet<String> dataSet, String fieldSeparator, boolean hasHeader, boolean doPreferDouble) throws Exception {
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
