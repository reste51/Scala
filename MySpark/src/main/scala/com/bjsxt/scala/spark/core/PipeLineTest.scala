package com.bjsxt.scala.spark.core

import org.apache.spark.sql.SparkSession

/**
  * 管道处理 模式
  *  两个transform 算子, 不是执行 map 后再执行 filter,  而是 每个管道的方式:  word --> map --> filter
  */
object PipeLineTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");

    // 由于 worker节点中executor没有 该scala类 因此会出错
//    val sc = SparkSession.builder().master("spark://192.168.240.141:7077").appName("pipe line").getOrCreate().sparkContext

    val sc = SparkSession.builder().master("local").appName("pipe line").getOrCreate().sparkContext

    val rdd = sc.textFile("./data/words").flatMap(_.split(" "))

    /**
      * 两个transform 算子
      */
    val rdd2 = rdd.map(word => {
      println("====== map =======" + word)
      word+"-- map"
    })

    val rdd3 = rdd2.filter( word =>{
      println("********** filter ***********" + word)
      true
    })

    rdd3.count()


    sc.stop()
  }
}
