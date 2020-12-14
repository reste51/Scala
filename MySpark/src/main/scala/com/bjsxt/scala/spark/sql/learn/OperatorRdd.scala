package com.bjsxt.scala.spark.sql.learn

import org.apache.spark.sql.SparkSession

/**
  * rdd转为 DF
  * 1.case class - use reflection
  * 2.编程式, StructType
  */
object OperatorRdd {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");
    val rootPath = "file:///E:\\excise\\scala_test\\data\\example";

    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local")
      .config("spark.some.config.option", "some-value").getOrCreate()

    import spark.implicits._
    // 1. reflection

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    case class Person(name:String, age:Integer)
    spark.sparkContext.textFile(s"$rootPath/people.txt").map(_.split(","))
      .map(attrArr=> Person(attrArr(0),attrArr(1).trim.toInt)).toDF()





  }
}
