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

package io.flink.ddf;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import org.apache.flink.api.java.DataSet;

/**
 * User: satya
 */

public class FlinkDDF extends DDF {


    public FlinkDDF(DDFManager manager, Object data, Class<?>[] typeSpecs, String namespace, String name, Schema schema) throws DDFException {
        super(manager, data, typeSpecs, namespace, name, schema);
    }

    public FlinkDDF(DDFManager manager, DDFManager defaultManagerIfNull) throws DDFException {
        super(manager, defaultManagerIfNull);
    }


    public FlinkDDF(DDFManager manager) throws DDFException {
        super(manager);
    }

    public DataSet<Object[]> getDataSetOfObjects() throws DDFException {
        return (DataSet<Object[]>) this.getRepresentationHandler().get(DataSet.class, Object[].class);
    }
}
