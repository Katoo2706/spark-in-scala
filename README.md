# Note
```
Spark 3.5.0 compatible with Scala 2.13.12 and JVM 11.

If error on Main, delete .idea and re import the project. 

If error with import error, choose JV 11 and reload sbt (right side)
```

![Spark APIs](https://cms.databricks.com/sites/default/files/inline-images/rdd-img-1.png)
# DataFrames
> A DataFrame is a Dataset organized into named columns
- Schema = list describing the column name and types:**
- DataFrameReader and Transformation are `lazy evalution`. It will only be executed when calling the action (e.g: show())
- DataFrames can be constructed from a wide array of sources such as: structured data files, tables in Hive, external databases, or existing RDDs
- In the Scala API, DataFrame is simply a type alias of Dataset[Row]

### SaveMode
1. Spark supports saving DataFrames to various file formats, such as Parquet, Avro, JSON, and more.
   1. Delta: `libraryDependencies += "io.delta" %% "delta-core" % "1.0.0"`
   2. Iceberg: `libraryDependencies += "org.apache.iceberg" % "iceberg-spark3" % "0.12.0"` [Spark Iceberg](https://iceberg.apache.org/spark-quickstart/)
2. When saving a DataFrame, Spark automatically partitions the data into multiple files for improved parallelism.
3. Saving in Parquet format is often recommended for optimal performance and storage efficiency.
4. Saving to cloud storage platforms like Amazon S3 or Hadoop Distributed File System (HDFS) is supported.

**Need to be distributed:**
- Data too big for a single computer
- Too long to process the entire data on a single CPU

**Partitioning:**
- splits the data into files, distributed between node in the cluster
- impacts the processing parallelism

