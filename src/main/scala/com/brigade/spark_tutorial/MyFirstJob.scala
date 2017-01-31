package com.brigade.spark_tutorial

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


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
    println("First rows of the CSV:")
    inputRDD.take(5).foreach { censusEntry =>
      println(s"\t$censusEntry")
    }

    //
    // Compute and print the counties with the most births
    //
    val birthsByCountyDesc =
      inputRDD.map { censusEntry =>
        (censusEntry.county.toLowerCase(), censusEntry.count)
      }
      .reduceByKey{ case(countLeft, countRight) => countLeft + countRight}
      .map { _.swap }
      .sortByKey(ascending = false)

    println("Counties with the most births:")
    birthsByCountyDesc.take(5).foreach { case (births, county) =>
        println(s"\t$county, $births")
    }

    //
    // Insert your code here:
    //    * Total number of births
    //    * Compute the ten most common names for boys and girls
    //    * The percent breakdown by gender
    //
    // See: https://spark.apache.org/docs/2.0.2/quick-start.html
  }
}

object MyFirstJob {
  def main(args: Array[String]) {
    val babyNamesInput = "data/BabyNamesNY.csv"
    val spark = SparkSession
      .builder()
      .appName("My first Spark job")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val job = new MyFirstJob(spark)
    job.run(babyNamesInput)
  }
}
