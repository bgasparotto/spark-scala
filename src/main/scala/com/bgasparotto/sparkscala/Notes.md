# Spark
- Built in Scala;
- DAG (Directed Acyclic Graph) engine optimises workflows
- Driver Program/Spark Context -> Cluster Manager (Spark or Hadoop YARN) -> n Executors (with Cache and Tasks)
- Spark CORE
	- Spark Streaming
	- Spark SQL
	- MLLib (Machine Learning, still quite basic but works fine for linear regression)
	- GraphX (Getting information on how the information and attributes connect)

## RDD: Resilient Distributed Dataset
- Giant set of data combined from different sources.
- Can be created from different sources, such as files, AWS, Hadoop HDFS, JDBC, etc
- Once you have the RDD, you can transform your data with:
	- map, flatmap, filter, distinct, sample, union, intersection, subtract, cartesian
	- Once you transformed, you can call an action. Only at this point Spark will execute your instructions:
 	- collect, count, countByValue, take, top, reduce and more...

## Coding
- It's a good practise to get rid of the unnecessary data as soon as possible.
 
## Spark Internals
- Spark creates an execution plan for your operation and then divide the steps into:
	- Stages that can be parallelised, then each stage is split into
		- Tasks that are distribued into individual nodes in your cluster
		- Theses tasks are scheduled across your cluster and executed
		
## Key/Values RDDs
- Maps are represented by tuples in Scala.
- If you need to map only the values of a Map, prefer mapValues() and flatMapValues() instead the normal map() and flatMap()

## Broadcast objects
- Better than loading everything in memory, specially if your dataset is massive.
- Broadcast objects to the executors, such that they are always there whenever needed.
- sc.broadcast() to broadcast and sc.value() to get it back

## Partitioning
- You would like to have at least as many partitions as the amount of cores * executors/workers.
- Too few partitions won't take advantage of the cluster, too few creates overhead of shuffling data.
- 100 partitions is a safe start for large operations.
- Use the method `partitionBy(new HashPartitioner(100)` for example;
- Should be used before running a large operation, like groupBy, reduce, combine, etc.

## Good practices
- Just use an empty, default SparkConf object in your driver, otherwise it will override other sources.
- Configuration precedence: script -> command line -> configuration file.
- YARN is the cluster manager that runs on top of Hadoop.
- Should use HDFS for exchanging data files and scripts.
