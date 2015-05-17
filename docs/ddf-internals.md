###DDF Core
A DDFManager is fetched through the name of its engine.

```
DDFManager.get(engineName) 
```

The relevant configuration for `engineName` is defined in the configuration.
This method fetches the class name of the DDFManager specified for the given 
engine and creates a new instance of it through no-args constructor. 
(no other constructor of DDFManager is used)

An implementation of DDFManager must implement the methods `getEngine` and `loadTable`.
The getEngine method should return the engineName. The loadTable method must read a file,
construct its DDF and return it.

DDFManager has 2 different methods to create DDF from given data - 

1. `sql2ddf` - The sql2ddf method eventually calls the `IHandleSql` implementation’s `sql2ddf` method. 
2. `newDDF` - internally, the private method newDDF is called. It has the signature,

    ```
    newDDF(Class<?>[] argTypes, Object[] argValues) 
    ```
            
    This method loads the custom DDF class (specified in config) and tries to find 
    a constructor accepting parameters of the types listed in argTypes (`ddfClass.getConstructor(argsTypes)`).
    If it exists, it is invoked with argValues to get a new DDF instance.

**When defining the constructor, the method *initialize (defined in DDF.java)* should be used to set manager and different handlers.
 The different handlers are created using the protected method *newHandler(Class<I> theInterface)*.**


####Spark Support
`SparkDDFManager`’s no-args constructor creates a SparkContext, a HiveContext using the SparkContext and 
configures its `spark.sql.inMemoryColumnarStorage.compressed` and `spark.sql.inMemoryColumnarStorage.batchSize` properties.

`SparkDDFManager`’s `loadTable` method works in the following manner

1. loads the file as `JavaRDD<String>`
2. evaluates the types of columns by taking a sample of data (utilizes `RDD.take` which results in a `List<String>`)
3. creates a table and loads given data using `sql2txt` and `sql2ddf` by passing relevant SQL statements. 


The Spark `SqlHandler` extends the abstract class `ASqlHandler( extends ADDFFunctionalGroupHandler and implements IHandleSql)`. 
The `SqlHandler`’s `sql2txt` method calls the underlying `HiveContext`’s `sql` method and returns the result as a `List<String>`.

The `SqlHandler`’s `sql2ddf` method works by
creating a DDF with no data and its representation is set to `SchemaRDD` by default. 
The sql query/command is executed through the `HiveContext` and its result is added to the 
newly created DDF by specifying its representation as `RDD[Row]`. 

**A single DDF may have simultaneously multiple representations, all of which are 
expected to be equivalent in terms of relevant content value.**

**To support conversion between different custom, we could extend *io.ddf.content.RepresentationHandler* and 
define how the transformation is done through *addConvertFunction*.** 

For Spark, `io.spark.ddf.content.RepresentationHandler` adds transformation from `RDD[Row]` to `SchemaRDD`.

The updated DDF is added to the DDFManager and returned from the method. 

SparkDDF stores reference to underlying RDD and has a getter method to access it.


