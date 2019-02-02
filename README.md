# Example Spark Application Source Code

This shows how to initialise a `SparkContext` and `HiveContext` (commonly called `sqlContext`) for reading and writing to Hive managed tables.

The example uses Spark 1.6.

If using Spark 2.0, the creation of contexts has moved to a `SparkSession` object, and the syntax for reading and writing csv's is slightly different.

Build the jar with Maven and use the command at the bottom to run the example.

#### Requirements

Please install hadoop for windows by cloning [this project](https://github.com/srccodes/hadoop-common-2.2.0-bin
) to `C:/Hadoop`. Add the filepath to a `HADOOP_HOME` environment variable and add `%HADOOP_HOME%/bin` to your `PATH` variable
(Restart after setting env variables).
##### Other requirements

Java 1.8 

Scala IntelliJ plugin enabled - SDK 2.10.7.
(Note I chose Scala as Spark is written in Scala)

Maven

We may want to update all versions to latest compatible versions at some stage, but this should be enough to get started with spark 1.6

### Testing

For unit testing, the project uses Junit, [ScalaTest](http://www.scalatest.org/) and [Spark Testing Base](https://github.com/holdenk/spark-testing-base/wiki
)

##### An example command for running this application on the edge node using Yarn

```
spark-submit /
	--aquaq.Example /
	--master yarn /
	--deploy-mode client /
	--executor-memory 1G /
	 --num-executors 1 /
	 --executor-cores 4 /
	 --driver-memory 1G /
	 --queue default /
	 example-1.0-SNAPSHOT-jar-with-dependencies.jar [optonal java args]
```

Please note: it won't actually do anything as the table it tries to read from does not exist.
However if you run the unit tests it will spin up a local Hadoop file system which the unit test will use.
