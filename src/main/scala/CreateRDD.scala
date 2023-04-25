package com.sparkExamples.practice
import org.apache.spark.sql.SparkSession

object CreateRDD {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()//it is from apache spark
      .appName("MyFirstRDD")// in a local [3] 2 run as exequter 1 is for the driver code
      .master("local[3]")// driver run in master where it is not a dristributed system both master and slave in a same sysytem
      .getOrCreate()// it will get are else it will create a new session
    val rdd =spark.sparkContext.parallelize(Seq(("Kavin",1),("Achsaya",2),("karan",3)))
    rdd.foreach(println)
    scala.io.StdIn.readLine()
  }

}
