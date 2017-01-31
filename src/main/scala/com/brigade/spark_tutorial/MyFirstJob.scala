package com.brigade.spark_tutorial

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}


case class CensusEntry(year: Int,
                firstName: String,
                county: String,
                sex: String,
                count: Int)

class MyFirstJob(spark: SparkSession) {
  import spark.implicits._

  def run(input: String)  = {
    //
    // Read in the census file
    //
    val inputRDD: RDD[CensusEntry] =
      spark
        .read
        .format("org.apache.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input)
        .as[CensusEntry]
        .rdd

    //
    // Let's print out the first five rows
    //
    inputRDD.take(5).foreach { censusEntry =>
      println(censusEntry)
    }

    //
    // Insert your code here:
    //    * Total number of births
    //    * Compute the ten most common names for boys and girls
    //    * The percent breakdown by gender
    //    * Number of births for the 10 largest cities
  }
}

object MyFirstJob {
  def main(args: Array[String]) {
    val babyNamesInput = "data/BabyNamesNY.csv"
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val job = new MyFirstJob(spark)
    job.run(babyNamesInput)
  }
}
