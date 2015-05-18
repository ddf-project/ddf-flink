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
import io.flink.ddf.utils.Utils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.conf.Configuration;
import org.apache.mrql.*;

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
            Config.trace_execution = true;
            Configuration conf = new Configuration();
            Config.write(conf);
            MRQLInterpreter.clean();
            Evaluator.evaluator = new MRQLInterpreter();
            Plan.conf = conf;
            Evaluator.evaluator.init(conf);
            this.env = ((MRQLInterpreter) Evaluator.evaluator).getExecutionEnvironment();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public DDF loadTable(String fileURL, String fieldSeparator) throws DDFException {
        try {
            DataSet<String> text = env.readTextFile(fileURL);
            Tuple3<String[], List<Schema.Column>, String[]> metaInfo = Utils.getMetaInfo(env, mLog, text, fieldSeparator, false, true);
            List<Schema.Column> metaInfoForSchema = metaInfo.f1;
            String tableName = getDummyDDF().getSchemaHandler().newTableName();
            Schema schema = new Schema(tableName, metaInfoForSchema);
            FlinkDDF flinkDDF = new FlinkDDF(this, text, new Class[]{DataSet.class, String.class}, null, tableName, schema);
            MR_flink mrFlink = (MR_flink) flinkDDF.getRepresentationHandler().get(MRData.class);
            //add a binding to mrFlink
            MRQLInterpreter.new_global_binding(flinkDDF.getTableName(), mrFlink);
            return flinkDDF;
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
