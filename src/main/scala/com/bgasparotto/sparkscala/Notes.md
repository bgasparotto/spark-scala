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
- The data files have to be present on each worker if being read directly from the file system.

## Coding
- It's a good practise to get rid of the unnecessary data as soon as possible.
 
## Spark Internals
- Spark creates an execution plan for your operation and then divide the steps into:
	- Stages that can be parallelised, then each stage is split into
		- Tasks that are distributed into individual nodes in your cluster
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
- The more stages you have, the more data shuffling will happen. Always aim for fewer stages.

## DataFrames
- DataFrames are RDDs that contain Row objects;
- DataFrames and DataSets are the same, in another words, a DataFrame is a DataSet of Row objects;
- DataFrames is the legacy terminology of Spark, but going forward, the term DataSet will be more popular;
- You can run SQL queries on top of it;
- The trend is to use more DataSets instead of RDDs wherever possible.
- On Spark 2+, you create SparkSession instead of SparkContext when using Spark SQL/DataSets.

## MLLib
- Very sensitive to the parameters chosen;
- Don't put your faith on black box solution, it's dodgy. Understand how it works or build your own;
- When analysing big data, small problems in algorithms become big ones;
- Very often, quality of the input data is the real issue;
- Good data always trump fancy algorithms, especially that fancy algorithms aren't always better.

## Linear Regression with Spark
- Spark streaming uses SGD (Stochastic Gradient Descent) for Linear Regression.
- SGD is friendlier with multi-dimensional data, e.g. you're trying to predict height and age based based on the input.
- A model is trained by providing a Tuple of a label (y axis: what you're trying to predict) and a vector of features (x axis: the variables associated to the label).
- The data has to be scaled down to 0 for training, and scaled up backwards given SGD doesn't handle scaling.
- Train Test is the technique of testing ML models:
    1. Randomly split the data into two and use one half to train the model: `DataFrame.randomSplit(Array(0.5, 0.5))`;
    2. Use the other half to test the model by comparing the model predictions with the actual resulting value.

## Spark Streaming (Micro-batch)
- Doesn't process data one-by-one, but in small chunks (small RDDs, micro-batch) every x time (e.g. every 1 second).
- Checkpoints to store the state to disk for fault tolerance.
- Windowed operations allow you to aggregate data of a longer period of time than short batch duration.
- Let you maintain state of a given arbitrary key.