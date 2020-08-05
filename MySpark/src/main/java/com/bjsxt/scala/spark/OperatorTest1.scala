package com.bjsxt.scala.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 算子实践类：
  * 1. transformation:
  *     RDD => RDD
  *     map   flatMap  filter reduceByKey  sortBy sortByKey sample
  * 2. action:
  *     会产生job, 触发 transform执行
  *    count collect first take
  */
object OperatorTest1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");

    val conf = new SparkConf()
    conf.setMaster("local").setAppName("OperatorTest1")

    val sc = new SparkContext(conf)
    // 读取一个文件, 注:IDEA 声明类型时, 输入).var 选择 指定type, 按回车
    val linesRdd: RDD[String] = sc.textFile("./data/words")
    // map -->  1 对 1的转化
    //    linesRdd.map(line => line+"@@@").foreach(println)

    // flatMap  1对 多
//    linesRdd.flatMap(line => line.split(" ")).foreach(println)

    // filter _ 查找hello单词   string => boolean
//      linesRdd.flatMap(_.split(" ")).filter("hello".equals(_)).foreach(println)

    //reduceByKey
//    linesRdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)

    //sortBy 第一个参数指定 排序的值:  tuple(String, Int)，  二为 正序/倒叙
    val wordCountRdd: RDD[(String, Int)] = linesRdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
//    wordCountRdd.sortBy(tp=>tp._2,false).foreach(println)

    // sortByKey:  (word,count) => (count, word) => sort => (word,count)
//    wordCountRdd.map(pair=>(pair._2,pair._1)).sortByKey(true).map(pair=>pair.swap).foreach(println)

    /*
      sample 抽样 (抽取后是否放回, 抽取比例，是否每次一样)
      0.1 每次会抽取 10%左右的数据， 每次条数会变化
     */
//    linesRdd.flatMap(_.split(" ")).sample(true,0.1).foreach(println)/
      // seed 代表每次抽取不会变化， 每台机器每个人抽取一致， 排除干扰
//      linesRdd.flatMap(_.split(" ")).sample(true,0.1,100L).foreach(println)

    /*********************action********************/
    // collect 会将结果返回 给Driver
//    wordCountRdd.collect().foreach(println)
    // first 查询的时  take(1)
//    println(wordCountRdd.first)

    // 前5条数据
    wordCountRdd.take(5).foreach(println)

//    sc.setLogLevel("Error")  // 设置日志级别
    sc.stop()
  }
}
