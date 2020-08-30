package com.bjsxt.scala.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
  * 广播变量
  * driver端声明变量 list,  executor(rdd )使用则发生 每个task 都会拥有 list的副本，如果list 内存过大，会导致executor 所需内存增加。
  *
  * Broadcast 会在executor 生成一个 BlockManager， 每个 task  都会获取它的值，不会额外产生副本;  因而内存减少;
  * 注意：1. 广播变量 不能呗 executor 修改。
  *       2. rdd不能被广播，只能 rdd.collect()回收结果到driver端广播。
  *
  * task的数量是根据  partition个数决定的
  */
object BroadcastDemo {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("BroadcastDemo").master("local").getOrCreate()
    val sc: SparkContext = session.sparkContext

    // driver端声明变量 list
    val list = List[String]("zhangsan", "wangwu")
    // 声明一个 广播变量
    val broadcastVar: Broadcast[List[String]] = sc.broadcast(list)

    val rdd = sc.parallelize(List[String]("zhangsan","liyong", "wangwu"))

    /*
       executor 执行 task
      executor(rdd )使用则发生 每个task 都会拥有 list的副本
     */
//    val rdd2 = rdd.filter(name => !list.contains(name))

    /*
     在executor 生成一个 BlockManager,每个 task  都会获取它的值，不会额外产生副本
     */
    val rdd2 = rdd.filter(one=>{
      val blockManagerList: List[String] = broadcastVar.value
      !blockManagerList.contains(one)
    })
    rdd2.foreach(println)



    sc.stop()
  }
}
