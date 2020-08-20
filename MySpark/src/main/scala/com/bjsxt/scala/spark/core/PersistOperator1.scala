package com.bjsxt.scala.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
;

/**
  * 持久化算子操作
  * cache 内存
  * persist -- StorageLevel
  * checkpoint
  */
object PersistOperator1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PersistOperator1").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd: RDD[String] = sc.textFile("./data/persistData.txt")
    /*
        存储内存_源码调用:persist(StorageLevel.MEMORY_ONLY)
        由于懒执行, startTime1 读取文件后才会缓存.
      */
    rdd = rdd.cache()

    val startTime1 = System.currentTimeMillis()
    // action算子触发  一次io, 读取本地文件
    val count=  rdd.count()

    val endTime1 = System.currentTimeMillis()
    println(s"第一次条数: $count, 查询时间: ${endTime1-startTime1}")

    // 注: 不能放入此处, 否则   第二次也会读取文件后才会存储到内存中
//    rdd = rdd.cache()

    val startTime2 = System.currentTimeMillis()
    val count2=  rdd.count()
    val endTime2 = System.currentTimeMillis()
    println(s"第二次条数: $count2, 查询时间: ${endTime2-startTime2}")

//    第二次条数: 5138965, 查询时间: 2475
//    第一次条数: 5138965, 查询时间: 2322
    sc.stop()
  }
}
