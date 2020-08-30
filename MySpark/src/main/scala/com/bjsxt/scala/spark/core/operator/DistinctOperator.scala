package com.bjsxt.scala.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
;

/**
  * distinct 算子的demo
  * 产生rdd的方法：
  * sc.textFile("")
  */
object DistinctOperator {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("DistinctOperator")
    val sc = new SparkContext(conf)


    val rdd: RDD[String] = sc.parallelize(List[String]("1","hello","hello","hello","word","word","wo"))
    // wordCount思路写distinct : foreach 为action 触发 transform 执行
    rdd.map(word=>(word,1)).reduceByKey((val1,val2)=> val1+val2).map(_._1).foreach(println)

    // 源码:     map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
//    rdd.distinct().foreach(println)

    sc.stop()
  }
}
