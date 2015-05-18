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

package io.flink.ddf.content;

import io.ddf.DDF;
import io.ddf.content.ConvertFunction;
import io.ddf.content.Representation;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import io.flink.ddf.FlinkDDFManager;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.mrql.*;

import java.io.Serializable;
import java.util.List;

/**
 * User: satya
 */
public class Conversions implements Serializable {

    public static class StringToObjectArr implements MapFunction<String, Object[]> {

        protected final Schema.Column[] columns;

        public StringToObjectArr(Schema.Column[] columns) {
            this.columns = columns;
        }

        @Override
        public Object[] map(String value) throws Exception {
            String[] valueStr = value.split(",");
            final int colSize = columns.length;
            Object[] values = new Object[colSize];
            for (int i = 0; i < colSize; i++) {
                values[i] = object(valueStr[i], columns[i]);
            }
            return values;
        }
    }

    public static Schema.Column[] getSchemaColumns(DDF ddf) {
        List<Schema.Column> columnList = ddf.getSchema().getColumns();
        Schema.Column[] cols = new Schema.Column[columnList.size()];
        return columnList.toArray(cols);
    }

    public abstract static class FlinkConvertFunction extends ConvertFunction {
        protected transient DDF ddf;

        public FlinkConvertFunction(DDF ddf) {
            super(ddf);
            this.ddf = ddf;
        }

        protected Schema.Column[] getColumns() {
            return getSchemaColumns(ddf);
        }
    }

    public static class StringDataSetToObjectArrDataSet extends FlinkConvertFunction {

        public StringDataSetToObjectArrDataSet(DDF ddf) {
            super(ddf);
        }

        @Override
        public Representation apply(Representation rep) throws DDFException {
            DataSet<String> textFile = (DataSet<String>) rep.getValue();
            final Schema.Column[] columns = getColumns();
            DataSet<Object[]> objects = textFile.map(new StringToObjectArr(columns));
            return new Representation(objects, DataSet.class, Object[].class);
        }


    }

    public static class ObjectArrDataSetToMRFlink extends FlinkConvertFunction {

        public ObjectArrDataSetToMRFlink(DDF ddf) {
            super(ddf);
        }

        @Override
        public Representation apply(Representation rep) throws DDFException {
            DataSet<Object[]> dataSet = (DataSet<Object[]>) rep.getValue();
            DataSet<FData> fDataDataSet = dataSet.map(new MapFunction<Object[], FData>() {
                @Override
                public FData map(Object[] objects) throws Exception {
                    Tuple tuple = new Tuple(objects.length);
                    Schema.Column[] columns = getColumns();
                    for (int i = 0; i < columns.length; i++) {
                        tuple.set(i, mrData(objects[i], columns[i]));
                    }
                    return new FData(tuple);
                }
            });
            MR_flink mrFlink = new MR_flink(fDataDataSet);
            return new Representation(mrFlink, MR_flink.class);
        }
    }

    public static class MRDataToObjectArr extends FlinkConvertFunction {

        public MRDataToObjectArr(DDF ddf) {
            super(ddf);
        }

        @Override
        public Representation apply(Representation rep) throws DDFException {
            MRData mrData = (MRData) rep.getValue();
            if (mrData instanceof MR_flink) {
                MR_flink mrFlink = (MR_flink) ddf.getRepresentationHandler().get(MRData.class);
                DataSet<FData> fDataDataSet = mrFlink.flink();
                DataSet<Object[]> dataSet = fDataDataSet.map(new MapFunction<FData, Object[]>() {
                    @Override
                    public Object[] map(FData fData) throws Exception {
                        Tuple tuple = (Tuple) fData.data();
                        Schema.Column[] columns = getColumns();
                        Object[] arr = new Object[tuple.size()];
                        for (int i = 0; i < columns.length; i++) {
                            arr[i] = object(tuple.get(i), columns[i]);
                        }
                        return arr;
                    }
                });
                return new Representation(dataSet, DataSet.class, Object[].class);
            } else {
                PersistenceHandler persistenceHandler = (PersistenceHandler) ddf.getPersistenceHandler();
                String dumpStr = String.format("dump '%s' from %s;", persistenceHandler.getDataFileName(), ddf.getTableName());
                MRQL.evaluate(dumpStr);
                FlinkDDFManager manager = (FlinkDDFManager) ddf.getManager();
                String pathToRead = persistenceHandler.getDataFileNameAsURI();
                DataSet<String> textFile = manager.getExecutionEnvironment().readTextFile(pathToRead);
                //add a string representation
                ddf.getRepresentationHandler().add(textFile, DataSet.class, String.class);
                //now get the object representation
                DataSet<Object[]> dataSet = (DataSet<Object[]>) ddf.getRepresentationHandler().get(DataSet.class, Object[].class);
                return new Representation(dataSet, DataSet.class, Object[].class);
            }
        }
    }

    public static Representation stringDataSet = new Representation(DataSet.class, String.class);
    public static Representation objectArrDataSet = new Representation(DataSet.class, Object[].class);
    public static Representation mr_data = new Representation(MRData.class);


    public static Object object(MRData value, Schema.Column column) {
        if (value == null) return null;
        else {
            return object(value, column.getType());
        }
    }

    public static Object object(String value, Schema.Column column) {
        if (value == null) return null;
        else {
            return object(value, column.getType());
        }
    }

    public static MRData mrData(Object value, Schema.Column column) {
        if (value == null) return null;
        else {
            return mrData(value, column.getType());
        }
    }

    public static MRData mrData(Object s, Schema.ColumnType columnType) {
        switch (columnType) {
            case INT:
                return new MR_int((Integer) s);
            case FLOAT:
                return new MR_float((Float) s);
            case DOUBLE:
                return new MR_double((Double) s);
            case LONG:
            case BIGINT:
                return new MR_long((Long) s);
            case LOGICAL:
                return new MR_bool((Boolean) s);
            default:
                return new MR_string(s.toString());
        }
    }

    public static Object object(MRData s, Schema.ColumnType columnType) {
        switch (columnType) {
            case INT:
                return ((MR_int) s).get();
            case FLOAT:
                return ((MR_float) s).get();
            case DOUBLE:
                return ((MR_double) s).get();
            case LONG:
            case BIGINT:
                return ((MR_long) s).get();
            case LOGICAL:
                return ((MR_bool) s).get();
            default:
                return ((MR_string) s).get();
        }
    }


    public static Object object(String s, Schema.ColumnType columnType) {
        switch (columnType) {
            case INT:
                return Integer.valueOf(s);
            case FLOAT:
                return Float.valueOf(s);
            case DOUBLE:
                return Double.valueOf(s);
            case LONG:
            case BIGINT:
                return Long.valueOf(s);
            case LOGICAL:
                return Boolean.valueOf(s);
            default:
                return s;
        }
    }

    public static Double asDouble(Object o, Schema.ColumnType columnType) {
        switch (columnType) {
            case INT:
            case FLOAT:
            case DOUBLE:
            case LONG:
            case BIGINT:
            case STRING:
                Double.valueOf(o.toString());
            default:
                throw new IllegalArgumentException(String.format("Cannot convert column of type %s to Double", columnType));
        }
    }

}
