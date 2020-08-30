package com.bjsxt.scala.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

/**
  * 累加器
  * 如果在 executor 使用 driver声明的变量count( 累加),  输出后也是0(始终是 driver声明的初值)
  *
  * 如果达到累加的目的，则需要 把 每个executor 累加的值放入到driver , 在driver 汇总每个executor 计算的结果
  *
  *
  */
object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    //注：  设置 hadoop 的home目录, 便于访问 winutils 工具
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");

    val session: SparkSession = SparkSession.builder().appName("AccumulatorDemo").master("local").getOrCreate()
    val sc: SparkContext = session.sparkContext

    // driver端声明变量, 用于统计词汇的总数
    var count = 0

    // 声明 累加器需要使用 sc 来创建
    val accumulator: LongAccumulator = sc.longAccumulator

    val wordsRdd = sc.textFile("./data/words")
    wordsRdd.flatMap(line=> line.split(" ")).foreach(word => {
//      count +=1  // driver变量累加无法_ 记录每个executor 累加的值
      accumulator.add(1)
    })

    // 此时会 输出0
//    println(s"total words count is $count")
    println(s"total words count is ${accumulator.value}")
    sc.stop()
  }
}
