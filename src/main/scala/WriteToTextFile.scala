package com.sparkExamples.practice

import org.apache.spark.sql.SparkSession

object WriteToTextFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder() //it is from apache spark
      .appName("Write Text File") // in a local [3] 2 run as exequter 1 is for the driver code
      .master("local[1]") // driver run in master where it is not a dristributed system both master and slave in a same sysytem
      .getOrCreate() // it will get are else it will create a new session
    val rdd = spark.sparkContext.wholeTextFiles("data/textFile1*.txt")
    rdd.foreach(println)
    //scala.io.StdIn.readLine()
  }
}
