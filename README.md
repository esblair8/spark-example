# Example Spark Application

This shows how to initialise a `SparkContext` and `HiveContext` (commonly called `sqlContext`) for reading and writing to Hive managed tables.

The example was upversioned from Spark 1.6 to 2.4

Build the jar with Maven and use the command at the bottom to run the example.

#### Requirements

Please install Hadoop for windows by cloning [this project](https://github.com/srccodes/hadoop-common-2.2.0-bin
) to `C:/Hadoop`. Add the filepath to a `HADOOP_HOME` environment variable and add `%HADOOP_HOME%/bin` to your `PATH` variable
(Restart after setting env variables).
##### Other requirements

Java 1.8 

Scala IntelliJ plugin enabled - SDK 2.11.8

### Testing

For unit testing, the project uses Junit, [ScalaTest](http://www.scalatest.org/) and [spark-fast-tests](https://github.com/MrPowers/spark-fast-tests)

In practice we will also want to create generic reader and writer classes, test these once and integrate into projects as needed.


##### An example command for running this application on the edge node using Yarn

```
spark-submit /
	--spark-starter.Example /
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
