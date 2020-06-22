package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * SqlContext 使用
  */
object SQLContextApp {
  def main(args: Array[String]): Unit = {

    //注：  设置 hadoop 的home目录, 便于访问 winutils 工具
//    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");


    //1 创建对应的Context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SQLContext 使用")
    sparkConf.setMaster("local[2]")  // 设置本地模式

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //2. 相关的操作_处理json
//    val path = "E:\\excise\\bigData\\courseFile\\test2.json"  // windows本地路径
    val path = args(0)  // 默认取第一个参数
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    //3. 关闭资源
    sc.stop()

  }
}
