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
import io.flink.ddf.Utils;
import org.apache.flink.api.java.DataSet;
import org.apache.mrql.MRData;
import org.apache.mrql.MRQL;

import java.util.List;

/**
 * User: satya
 */
public class SqlHandler extends ASqlHandler {

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
        String tableName = this.getDDF().getSchemaHandler().newTableName();
        if (schema != null)
            schema.setTableName(tableName);
        MRQL.evaluate(String.format("store %s := %s;", tableName, command));
        MRData data = MRQL.lookup_global_binding(tableName);
        DDF ddf = new FlinkDDF(this.getManager(), data, null, null, schema.getTableName(), schema);
        //will dump this as a CSV.
        MRQL.evaluate(String.format("dump %s from %s;", tableName, tableName));
        FlinkDDFManager manager = (FlinkDDFManager) this.getManager();
        DataSet<String> textFile = manager.getExecutionEnvironment().readTextFile(tableName, ",");
        //add a representation as a Flink DataSet
        ddf.getRepresentationHandler().add(textFile, DataSet.class, String.class);
        return ddf;
    }


    @Override
    public List<String> sql2txt(String command) throws DDFException {
        return sql2txt(command, Integer.MAX_VALUE);
    }

    @Override
    public List<String> sql2txt(String command, Integer maxRows) throws DDFException {
        return sql2txt(command, maxRows, null);
    }

    @Override
    public List<String> sql2txt(String command, Integer maxRows, String dataSource) throws DDFException {
        DDF ddf = sql2ddf(command, null, dataSource, null);
        DataSet<String> textFile = (DataSet<String>) ddf.getRepresentationHandler().get(DataSet.class, String.class);
        try {
            FlinkDDFManager manager = (FlinkDDFManager) this.getManager();
            return Utils.collect(manager.getExecutionEnvironment(), textFile.first(maxRows));
        } catch (Exception e) {
            throw new DDFException(e);
        }
    }
}
