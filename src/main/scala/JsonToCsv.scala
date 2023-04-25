package com.sparkExamples.practice

import org.apache.spark.sql.SparkSession

object JsonToCsv {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Json to csv")
      .master("local[1]")
      .getOrCreate()
    val df = spark.read.option("multiline", "true")
      .json("data/sample1.json")
   // df.printSchema()
   // df.show()
    df.show(false)
    df.write.csv("data/zipcodes.csv")
    //df.write.csv("data/empnew3")


  }
}
