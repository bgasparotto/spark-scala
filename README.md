# Spark with Scala
Repo for sharing my Spark with Scala studies.
The source code also includes example classes and datasets provided as part as some courses and books materials.

## Running the project
Package it with SBT
```shell script
cd spark-scala
sbt package
```

Run Spark master and worker with docker-compose:
```shell script
docker-compose up -d
```

Use [SDKMAN](https://bgasparotto.com/dont-install-java-by-yourself-anymore-use-sdkman-instead) to install Spark locally so you can access its tools:
```shell script
sdk install spark 2.4.4
```

Run your application with `spark-submit`:
```shell script
spark-submit --class com.bgasparotto.sparkscala.MostPopularSuperhero target/scala-2.11/spark-scala-assembly-0.1.jar 
```

Spark UI should be accessible at http://localhost:8080
