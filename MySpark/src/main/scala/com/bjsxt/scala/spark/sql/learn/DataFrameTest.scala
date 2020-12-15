package com.bjsxt.scala.spark.sql.test

import com.bjsxt.scala.spark.hive.Person
import org.apache.spark.sql.SparkSession

/**
  * Spark 官网学习
  * A DataFrame is a Dataset organized into named columns. is represented by a Dataset of Rows.(rows 类型的DS)
  * In the Scala API, DataFrame is simply a type alias of Dataset[Row]. While, in Java API, users need to use Dataset<Row> to represent a DataFrame.
  */
object DataFrameTest {
  def main(args: Array[String]): Unit = {
    // 注： 设置了hadoop-home 之后默认会 查找 hdfs 中的文件,读取本地文件路径:  需要使用 file:///E:/xxxx/xxx
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");

    val path = "file:///"+  this.getClass.getClassLoader.getResource("data/people.json").getPath()

    /*
      The entry point into all functionality in Spark is the SparkSession class. To create a basic SparkSession, just use SparkSession.builder():
     */
    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local").getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._


    //create DataFrames from an existing RDD, from a Hive table, or from Spark data sources.
    val df = spark.read.json(path).as[Person]
    df.show()

    // Print the schema in a tree format
    df.printSchema()

    // Select only the "name" column
    df.select("name").show()

    // Select everybody, but increment the age by 1
    df.select($"name",$"age"+1).show()

    // Select people older than 21
    df.filter($"age" > 21).show()

    // Count people by age
    df.groupBy($"age").count().show()


  }
}
