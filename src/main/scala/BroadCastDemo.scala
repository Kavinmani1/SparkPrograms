package com.sparkExamples.practice

import org.apache.spark.sql.SparkSession

object BroadCastDemo {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("BroadCast Demo")
      .master("local[1]")
      .getOrCreate()
    val inputRdd =spark.sparkContext.parallelize(Seq(("Emp1","100000","USA","NY"),("Emp2","200000","IND","TS"),("Emp3","100000","USA","TN"),("Emp4","110000","USA","TX"),("Emp5","500000","AUS","QUE")))
    //inputRdd.foreach(println)
    val countryData =Map(("USA","United States Of America"),("IND","India"),("AUS","Australia"))
    //countryData.foreach(println)
    val stateData =Map(("NY","Newyork"),("TS","Telangana"),("TN","Tamilnadu"),("TX","Texas"),("QUE","QueensLand"))
    //stateData.foreach(println)
    val broadcastStates = spark.sparkContext.broadcast(stateData)
    val broadcastCountries = spark.sparkContext.broadcast(countryData)

    val Finalrdd = inputRdd.map(f => {
      val country = f._3
      val state = f._4
      val fullCountry = broadcastCountries.value.get(country).get
      val fullState = broadcastStates.value.get(state).get
      (f._1, f._2, fullCountry, fullState)
  })
    println(Finalrdd.collect().mkString("\n"))
    scala.io.StdIn.readLine()

  }
}
