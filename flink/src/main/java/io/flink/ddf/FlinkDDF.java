package io.flink.ddf;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import org.apache.flink.api.scala.DataSet;
import org.apache.flink.api.table.Row;

public class FlinkDDF extends DDF {

    private DataSet<Row> dataSet = null;

    public FlinkDDF(DDFManager manager, Object data, Class<?>[] typeSpecs, String namespace, String name, Schema schema) throws DDFException {
        super(manager, data, typeSpecs, namespace, name, schema);
        this.dataSet = (DataSet<Row>) data;
    }

    public FlinkDDF(DDFManager manager, DDFManager defaultManagerIfNull) throws DDFException {
        super(manager, defaultManagerIfNull);
    }


    public FlinkDDF(DDFManager manager) throws DDFException {
        super(manager);
    }


    public DataSet<Row> getDataSet() {
        return this.dataSet;
    }

    public DataSet<java.lang.Object[]> getDataSetOfObjects() throws DDFException {
        return (DataSet<Object[]>) this.getRepresentationHandler().get(DataSet.class, Object[].class);
    }
}
