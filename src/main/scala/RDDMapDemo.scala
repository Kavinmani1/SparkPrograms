package com.sparkExamples.practice

import org.apache.spark.sql.SparkSession

object RDDMapDemo {
  def main(args: Array[String]): Unit = {
    val Spark=SparkSession.builder()
      .appName("DemoMap method") // in a local [3] 2 run as exequter 1 is for the driver code
      .master("local[1]") // driver run in master where it is not a dristributed system both master and slave in a same sysytem
      .getOrCreate()
    val data = Seq("Project Gutenberg’s",
      "Alice’s Adventures in Wonderland",
      "Project Gutenberg’s",
      "Adventures in Wonderland",
      "Project Gutenberg’s")

    import Spark.sqlContext.implicits._
    val df = data.toDF("data")
    df.show()
    val mapDF = df.map(fun => {// it is a one to one that is narrow transformation
      fun.getString(0).split(" ")
    })
    mapDF.show()
  }

}
