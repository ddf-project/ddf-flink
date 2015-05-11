package org.apache.mrql;

import io.ddf.content.Schema;
import org.apache.mrql.gen.Node;
import org.apache.mrql.gen.Tree;
import org.apache.mrql.gen.VariableLeaf;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

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

    public static List<Schema.Column> getSchemaColumns() {
        List<Schema.Column> columns = new ArrayList<>();
        addColumns(columns, topLevelQueryType());
        return columns;
    }

    public static void addColumns(List<Schema.Column> columns, Tree tree) {
        if (tree.is_node()) {
            Node node = (Node) tree;
            if (node.name().equalsIgnoreCase("bind")) {
                //these are the actual columns.
                Tree colName = node.children.head;
                Tree colType = node.children.tail.head;
                columns.add(new Schema.Column(colName.stringValue(), colType.stringValue()));
            } else {
                addColumns(columns, node.children.head);
                for (Tree kid : node.children.tail) {
                    addColumns(columns, kid);
                }
            }
        } else {
            if (tree.is_double()) {
                columns.add(new Schema.Column("VDouble", "double"));
            } else if (tree.is_long()) {
                columns.add(new Schema.Column("VLong", "long"));
            } else if (tree.is_string()) {
                columns.add(new Schema.Column("VString", "string"));
            } else if (tree.is_variable()) {
                VariableLeaf variableLeaf = (VariableLeaf) tree;
                columns.add(new Schema.Column("V" + variableLeaf.value, variableLeaf.value));
            }
        }

    }

}
