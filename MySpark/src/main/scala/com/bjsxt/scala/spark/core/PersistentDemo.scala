package com.bjsxt.scala.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
;

/**
  * 持久化算子
  */
object PersistentDemo {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");


    val conf = new SparkConf();
    conf.setAppName("persistentDemo")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("./data/words")
    val errorsRdd: RDD[String] = lines.filter(_.startsWith("ERROR"))

    val errors = errorsRdd.filter(_.contains("MYSQL")).count()
    val errors2 = errorsRdd.filter(_.contains("Http")).count()



  }
}
