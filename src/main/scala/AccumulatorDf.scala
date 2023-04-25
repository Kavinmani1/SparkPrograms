package com.sparkExamples.practice

import org.apache.spark.sql.SparkSession

object AccumulatorDf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AccumuulatorDemo")
      .master("local[1]")
      .getOrCreate()


  }
