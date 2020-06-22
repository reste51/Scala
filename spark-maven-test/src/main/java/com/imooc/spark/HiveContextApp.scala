package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * HiveContext的使用
  * 使用时 需要通过 --jars 把mysql驱动传递,  连接hive的元数据存储在mysql
  */
object HiveContextApp {
  def main(args: Array[String]): Unit = {

    //注：  设置 hadoop 的home目录, 便于访问 winutils 工具
    //    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");


    //1 创建对应的Context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SQLContext 使用")
    sparkConf.setMaster("local[2]")  // 设置本地模式
    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc) // spark 1.x使用

    //2. 相关的操作_处理json
    hiveContext.table("emp").show()

    //3. 关闭资源
    sc.stop()



  }
}
