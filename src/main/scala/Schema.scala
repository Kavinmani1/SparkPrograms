package com.sparkExamples.practice

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Schema {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("print schema")
      .master("local[1]")
      .getOrCreate()
    val simpleData = Seq(Row("James", "", "Smith", "36636", "M", 3000),
      Row("Michael", "Rose", "", "40288", "M", 4000),
      Row("Robert", "", "Williams", "42114", "M", 4000),
      Row("Maria", "Anne", "Jones", "39192", "F", 4000),
      Row("Jen", "Mary", "Brown", "", "F", -1))

    val simpleSchema = StructType(Array(
      StructField("firstname", StringType, true),
      StructField("middlename", StringType, true),
      StructField("lastname", StringType, true),
      StructField("id", StringType, true),
      StructField("gender", StringType, true),
      StructField("salary", IntegerType, true)
    ))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(simpleData), simpleSchema)
    df.printSchema()
    df.show()
    scala.io.StdIn.readLine()


}
}
