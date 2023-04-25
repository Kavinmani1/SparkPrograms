package com.sparkExamples.practice

import org.apache.spark.sql.SparkSession

object AccumuulatorDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AccumuulatorDemo")
      .master("local[1]")
      .getOrCreate()

    val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")
    val DoubAcc= spark.sparkContext.doubleAccumulator("SumAccumulatorDouble")
    val collAcc= spark.sparkContext.collectionAccumulator("SumAccumulatorDouble")
    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3))
    rdd.foreach(x => longAcc.add(x))
    rdd.foreach((y =>DoubAcc.add(y)))
   // rdd.foreach((z =>collAcc.add()))
    longAcc.add(1)
    longAcc.add(2)
    longAcc.add(4)
    longAcc.sum
    println(longAcc.value)
    println(DoubAcc.value)
    scala.io.StdIn.readLine()

  }

}
