package com.sparkExamples.practice

import org.apache.spark.sql.SparkSession

object PairedRDD {
  def main(args: Array[String]): Unit = {
    var spark=SparkSession.builder()
      .appName("paired rdd")
      .master("local[1]")
      .getOrCreate()
    val rdd = spark.sparkContext.parallelize(List("Germany India USA", "USA India Russia", "India Brazil Canada China"))
    val newrdd1=rdd.flatMap(_.split(" "))
    val pairRDD = newrdd1.map(f=>(f,1))

    pairRDD.distinct().foreach(println)
    println("Distinct ==>")


    pairRDD.sortByKey().foreach(println)
    println("Sort by Key ==>")


    pairRDD.reduceByKey((a,b)=>a+b).foreach(println)
    println("Reduce by Key ==>")

    def param1= (accu:Int,v:Int) => accu + v
    def param2= (accu1:Int,accu2:Int) => accu1 + accu2

    pairRDD.aggregateByKey(0)(param1,param2).foreach(println)
    println("Aggregate by Key ==> wordcount")

    pairRDD.keys.foreach(println)
    println("Keys ==>")

    pairRDD.values.foreach(println)
    println("values ==>")

    println("Count :" + pairRDD.count())

    pairRDD.collectAsMap().foreach(println)
    println("collectAsMap ==>")

    scala.io.StdIn.readLine()

  }


}
