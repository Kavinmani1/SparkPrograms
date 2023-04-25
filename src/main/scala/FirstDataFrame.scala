package com.sparkExamples.practice

import org.apache.spark.sql.SparkSession

object FirstDataFrame {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("DF Demo")
      .master("local[2]")
      .getOrCreate()
    val data =Seq(("Anil","1000",29,"Hyderabad"),
      ("karan","10000",24,"Karur"),
      ("kavin","2000",22,"Surat"),
      ("kumar","112000",25,"Guntur"),
      ("raj","51000",23,"Vizag"))
    val colms=Seq("name","salary","age","place")
    import spark.implicits._// this methord would have spark data frame functions
  val rdd =spark.sparkContext.parallelize(data)
    val df=rdd.toDF(colms:_*)
    df.show()

  }

}
