package com.bjsxt.scala.spark.sql.learn

import org.apache.spark.sql.SparkSession

object DataFrame01 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");
    val rootPath = "file:///E:\\excise\\scala_test\\data\\example";

//    The entry point into all functionality in Spark is the SparkSession class.
    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local")
      .config("spark.some.config.option", "some-value").getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames

      val df = spark.read.json(s"$rootPath/people.json")
      df.show()

    // Print the schema in a tree format
      df.printSchema()

    // Select only the "name" column
      df.select("name").show()
    /*
     session-scoped and will disappear if the session that creates it terminates
     */
    df.createOrReplaceTempView("people")
    //returns the result as a DataFrame.
    val sqlDf = spark.sql("select * from people where age > 20")
    sqlDf.show()

    /*
    all sessions , global_temp.customerViewName

     If you want to have a temporary view that is shared among all sessions and keep alive
      until the Spark application terminates, you can create a global temporary view.

      Global temporary view is tied to a system preserved database global_temp,
      and we must use the qualified name to refer it, e.g. SELECT * FROM global_temp.view1.
     */
    df.createGlobalTempView("people")
    spark.sql("select * from global_temp.people where age is null").show()

    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()


  }
}
