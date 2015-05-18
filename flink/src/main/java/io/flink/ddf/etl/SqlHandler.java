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

package io.flink.ddf.etl;

import io.ddf.DDF;
import io.ddf.content.Schema;
import io.ddf.etl.ASqlHandler;
import io.ddf.exception.DDFException;
import io.flink.ddf.FlinkDDF;
import io.flink.ddf.FlinkDDFManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.mrql.Bag;
import org.apache.mrql.MRData;
import org.apache.mrql.MRQLInterpreter;
import org.apache.mrql.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * User: satya
 */
public class SqlHandler extends ASqlHandler {
    static final String extractArgsRegex = "([^,]+\\(.+?\\))|([^,]+)";
    static final Pattern argsPattern = Pattern.compile(extractArgsRegex);
    static final Pattern words = Pattern.compile("\\w*");

    public SqlHandler(DDF theDDF) {
        super(theDDF);
    }

    @Override
    public DDF sql2ddf(String command) throws DDFException {
        return this.sql2ddf(command, null, null, null);
    }

    @Override
    public DDF sql2ddf(String command, Schema schema) throws DDFException {
        return this.sql2ddf(command, schema, null, null);
    }

    @Override
    public DDF sql2ddf(String command, Schema.DataFormat dataFormat) throws DDFException {
        return this.sql2ddf(command, null, null, dataFormat);
    }

    @Override
    public DDF sql2ddf(String command, Schema schema, String dataSource) throws DDFException {
        return this.sql2ddf(command, schema, dataSource, null);
    }

    @Override
    public DDF sql2ddf(String command, Schema schema, Schema.DataFormat dataFormat) throws DDFException {
        return this.sql2ddf(command, schema, null, dataFormat);
    }

    @Override
    public DDF sql2ddf(String command, Schema schema, String dataSource, Schema.DataFormat dataFormat) throws DDFException {
        if (command == null) throw new IllegalArgumentException("Statement cannot be null");
        command = command.trim();

        String tableName = schema != null ? schema.getTableName() : null;
        if (tableName == null) tableName = this.getDDF().getSchemaHandler().newTableName();
        if (schema == null) {
            schema = new Schema(tableName, (String) null);
        }
        schema.setTableName(tableName);

        Tuple2<MRData, List<Schema.Column>> result = getResult(command, tableName);
        MRData data = result.f0;
        schema.setColumns(result.f1);
        FlinkDDFManager manager = (FlinkDDFManager) this.getManager();
        return new FlinkDDF(manager, data, new Class[]{MRData.class}, null, tableName, schema);
    }

    private Tuple2<MRData, List<Schema.Column>> getResult(String command, String tableName) {

        if (!command.endsWith(";")) command += ";";
        ///This whole deal with static methods is lousy really
        //MRQL is not well designed. Our interpreter just extends from one of the existing ones.
        String toEval = String.format("store %s := %s", tableName, command);
        MRQLInterpreter.evaluate(toEval);
        List<Schema.Column> columns = MRQLInterpreter.getSchemaColumns();
        MRData data = MRQLInterpreter.lookup_global_binding(tableName);
        return new Tuple2<>(data, columns);
    }


    @Override
    public List<String> sql2txt(String command) throws DDFException {
        return sql2txt(command, null);
    }

    @Override
    public List<String> sql2txt(String command, Integer maxRows) throws DDFException {
        return sql2txt(command, maxRows, null);
    }

    @Override
    public List<String> sql2txt(String command, Integer maxRows, String dataSource) throws DDFException {
        maxRows = maxRows == null ? Integer.MAX_VALUE : maxRows;
        String tableName = this.getDDF().getSchemaHandler().newTableName();
        Tuple2<MRData, List<Schema.Column>> result = getResult(command, tableName);
        MRData data = result.f0;
        List<String> strings;
        if (data instanceof Bag) {
            Bag bag = (Bag) data;
            strings = new ArrayList<>(bag.size());
            int count = 0;
            for (MRData mrData : bag) {
                Tuple tuple = (Tuple) mrData;
                String[] row = new String[tuple.size()];
                for (short i = 0; i < tuple.size(); i++) {
                    row[i] = tuple.get(i).toString();
                }
                strings.add(StringUtils.join(row, ","));
                count++;
                if (count >= maxRows) break;
            }
        } else {
            strings = new ArrayList<>(1);
            strings.add(data.toString());
        }
        return strings;
    }

}
