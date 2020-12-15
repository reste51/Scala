package com.bjsxt.scala.spark.sql.test

import com.bjsxt.scala.spark.hive.Person
import org.apache.spark.sql.SparkSession

/**
  * spark 官网学习
  */
object DataSetTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");
    //file:///  使spark 查找本地文件 - resources 目录中的文件
    val path = "file:///"+  this.getClass.getClassLoader.getResource("data/people.json").getPath()


    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
    import spark.implicits._

    val caseClassDs = Seq(Person("Andy",32)).toDS()
    caseClassDs.show()

    val primitiveDS = Seq(1,2,3).toDS()
    val arr = primitiveDS.map(_ +1).collect()
    arr.foreach(print)

    //DF can be converted to DS  by provided a class
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()


  }
}



