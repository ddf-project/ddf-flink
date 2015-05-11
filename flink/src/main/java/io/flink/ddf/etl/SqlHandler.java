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
import io.flink.ddf.content.PersistenceHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.mrql.*;

import java.util.ArrayList;
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
        DDF ddf = new FlinkDDF(manager, data, new Class[]{MRData.class}, null, tableName, schema);
        addStringRepresentation(tableName, ddf);
        addTupleRepresentation(tableName, ddf);
        return ddf;
    }

    private Tuple2<MRData, List<Schema.Column>> getResult(String command, String tableName) {
        if (!command.endsWith(";")) command += ";";
        ///This whole deal with static methods is lousy really
        //MRQL is not well designed. Our interpreter just extends from one of the existing ones.
        MRQLInterpreter.evaluate(String.format("store %s := %s", tableName, command));
        List<Schema.Column> columns = MRQLInterpreter.getSchemaColumns();
        MRData data = MRQLInterpreter.lookup_global_binding(tableName);
        return new Tuple2<>(data, columns);
    }

    private void addTupleRepresentation(String tableName, DDF ddf) throws DDFException {
        //TODO How do we get the Flink Dataset representation.
        //This should be a tuple. We have the DDF schema with us as a result of getting the query type
        //Now we need to make it into a DataSet<Tuple>.
        //We will need a dynamic TupleBuilder Function which can then be used to map each line of the file to a tuple
    }

    private void addStringRepresentation(String tableName, DDF ddf) throws DDFException {
        //will dump this as a CSV.
        PersistenceHandler persistenceHandler = (PersistenceHandler) ddf.getPersistenceHandler();
        String dumpStr = String.format("dump '%s' from %s;", persistenceHandler.getDataFileName(), tableName);
        MRQL.evaluate(dumpStr);
        FlinkDDFManager manager = (FlinkDDFManager) this.getManager();
        String pathToRead = persistenceHandler.getDataFileNameAsURI();
        DataSet<String> textFile = manager.getExecutionEnvironment().readTextFile(pathToRead);
        ddf.getRepresentationHandler().add(textFile, DataSet.class, String.class);
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
