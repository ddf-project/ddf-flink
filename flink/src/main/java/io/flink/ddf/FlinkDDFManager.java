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
import org.apache.hadoop.conf.Configuration;
import org.apache.mrql.*;

import java.security.SecureRandom;
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
            Configuration conf = new Configuration();
            Config.write(conf);
            MRQL.clean();
            Evaluator.evaluator = new FlinkEvaluator();
            Plan.conf = conf;
            Evaluator.evaluator.init(conf);
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
            DataSet<String> text = env.readTextFile(fileURL);
            Tuple3<String[], List<Schema.Column>, String[]> metaInfo = Utils.getMetaInfo(env, mLog, text, fieldSeparator, false, true);
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


}
