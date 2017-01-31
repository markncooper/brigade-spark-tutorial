## Very Simple Spark Tutorial

Before you use this, first set up Intellij (or your favorite dev environment, then clone this repo.

To run locally install Spark. On a Mac:

> brew install apache-spark

To build execute:

> mvn package

To run:

> spark-submit --class com.brigade.spark_tutorial.MyFirstJob --master local target/spark_tutorial-SNAPSHOT-1.jar