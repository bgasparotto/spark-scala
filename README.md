# Spark with Scala
Repo for sharing my Spark with Scala studies.
The source code also includes example classes and datasets provided as part as some courses and books materials.

## Running the project
Assembly it with SBT
```shell script
cd spark-scala
sbt assembly
```

Run Spark master and worker with docker-compose:
```shell script
docker-compose up -d
```

### Running your application from Intellij IDEA:
Under _Edit Configurations_ of your runnable class, add the following on _VM options:_:
```jvm
-Dspark.master=local[*]
```

### Running your application on the cluster with `spark-submit`:
```shell script
docker container exec -it spark-master bash ./spark/bin/spark-submit --master spark://spark-master:7077 --class com.bgasparotto.sparkscala.MostPopularSuperhero /app/spark-scala-assembly-0.1.jar
```

Spark UI should be accessible at http://localhost:8080
