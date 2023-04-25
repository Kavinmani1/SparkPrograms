package com.sparkExamples.practice

import org.apache.spark.sql.SparkSession

object ReadCsv {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF Demo")
      .master("local[2]")
      .getOrCreate()
    val df=spark.read.option("header",true)
      .csv("data/emp.csv")
    df.show()
    df.select("name","age").where("age>23").show()
  }

}
