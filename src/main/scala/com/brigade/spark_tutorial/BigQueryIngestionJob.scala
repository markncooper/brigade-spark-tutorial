package com.brigade.spark_tutorial

import com.spotify.spark.bigquery._
import org.apache.spark.sql.SparkSession

class BigQueryIngestionJob(spark: SparkSession) {
  def run()  = {
    import org.apache.spark.sql.SQLContext
    val sqlContext = new SQLContext(spark.sparkContext)

    val profilesDF = sqlContext
      .read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://mysql02.bri.prod.iad.brigade.zone/brigade?user=brigade_readonly&password=<password>")
      .option("dbtable", "profiles")
      .option("pushdown", "true")
      .load()

    //    val schema = profilesDF.schema.map { field =>
    //      val name = field.name
    //      val dataType = field.dataType match {
    //        case BooleanType => "BOOLEAN"
    //        case ByteType => "BYTES"
    //        case DateType => "DATE"
    //        case DoubleType => "FLOAT"
    //        case FloatType => "FLOAT"
    //        case IntegerType => "INTEGER"
    //        case LongType => "INTEGER"
    //        case ShortType => "INTEGER"
    //        case StringType => "STRING"
    //        case TimestampType => "TIMESTAMP"
    //        case _ => "STRING"
    //      }
    //      val mode = if (field.nullable) {
    //        Some("NULLABLE")
    //      } else {
    //        Some("REQUIRED")
    //      }
    //      SchemaField(name, dataType, mode)
    //    }
    //
    //    import org.json4s._
    //    import org.json4s.jackson.Serialization
    //    import org.json4s.jackson.Serialization.write
    //    implicit val formats = Serialization.formats(NoTypeHints)
    //
    //    val schemaJson = write(schema)

    sqlContext.setGcpJsonKeyFile("/Users/jim/Downloads/Brigade-RC-96161110ed8e.json")
    sqlContext.setBigQueryProjectId("brigade-rc-media")
    sqlContext.setBigQueryGcsBucket("brigade_mysql")

    profilesDF.saveAsBigQueryTable("brigade-rc-media:brigade_mysql.profiles")
  }
}

object BigQueryIngestionJob {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("BigQuery")
      .getOrCreate()

    val job = new BigQueryIngestionJob(spark)
    job.run()
  }
}
