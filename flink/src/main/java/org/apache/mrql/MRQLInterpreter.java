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

package org.apache.mrql;

import org.apache.mrql.gen.Tree;

import java.io.IOException;
import java.io.StringReader;

/**
 * User: satya
 */
public class MRQLInterpreter extends FlinkEvaluator {
    /**
     * evaluate an MRQL query in a string
     *
     * @param query a string that contains an MRQL query
     * @return the evaluation result
     */
    public static MRData query(String query) {
        evaluate("store tt := " + query + ";");
        return variable_lookup("tt", global_env);
    }

    /**
     * evaluate MRQL statments in a string
     *
     * @param command a string that contains MRQL commands separated by ;
     */
    public static void evaluate(String command) {
        try {
            MRQLLex scanner = new MRQLLex(new StringReader(command));
            MRQLParser parser = new MRQLParser(scanner);
            parser.setScanner(scanner);
            MRQLLex.reset();
            parser.parse();
        } catch (Exception x) {
            x.printStackTrace();
            throw new Error(x);
        }
    }

    /**
     * clean up the MRQL workspace
     */
    public static void clean() {
        try {
            Plan.clean();
        } catch (IOException ex) {
            throw new Error("Failed to clean-up temporary files");
        }
    }

    public static Tree topLevelQueryType() {
        return TopLevel.query_type;
    }
}
