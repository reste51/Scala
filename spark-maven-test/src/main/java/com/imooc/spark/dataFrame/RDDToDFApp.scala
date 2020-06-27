package com.imooc.spark.dataFrame

import org.apache.spark.sql.SparkSession


/**
  * RDD 转为 DF， RDD 使用已知字段 case class 转为DF
  *  The case class defines the schema of the table. The names of the arguments to the case class are read using reflection and become the names of the columns.
  */
object RDDToDFApp {
  def main(args: Array[String]): Unit = {
    // For implicit conversions from RDDs to DataFrames

   val session = SparkSession.builder().appName("RDDToDFApp").master("local[2]").getOrCreate()

   val rdd =  session.sparkContext.textFile("E:\\excise\\bigData\\people.txt")

    //  For implicit conversions from RDDs to DataFrames
    import session.implicits._
    val personDf= rdd.map(_.split(",")).map(attributes  => Person(attributes (0),attributes (1).trim.toInt)).toDF
    personDf.show()
    personDf.printSchema()

    personDf.filter("age > 29").show()

    session.stop()
  }

  // you can use custom classes that implement the Product interface
  case class Person(name:String, age:Int)
}
