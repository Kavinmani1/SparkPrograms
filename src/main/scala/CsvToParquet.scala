package com.sparkExamples.practice

import org.apache.spark.sql.SparkSession

object CsvToParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("csv to json")
      .master("local[1]")
      .getOrCreate()
    val dfFromCSV = spark.read.option("header", true)
      .csv("data/emp.csv")
    //dfFromCSV.printSchema()
    //dfFromCSV.show(false)
    dfFromCSV.write.parquet("data/parquet1")


  }
}