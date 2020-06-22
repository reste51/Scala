package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * SparkSession的使用
  */
object SparkSessionApp {
  def main(args: Array[String]): Unit = {

    //注：  设置 hadoop 的home目录, 便于访问 winutils 工具
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");

    val sparkSession = SparkSession.builder().appName("SparkSession应用").
      master("local[2]").getOrCreate()

    val path = "E:\\excise\\bigData\\courseFile\\test2.json"  // windows本地路径
    //  效果一样  sparkSession.read.format("json")
    val people = sparkSession.read.json(path)
//    people.show()

    // 注册一个临时表, 使用sql语句
    people.registerTempTable("test")
    sparkSession.sql("select * from test where name='Yin'").show()

    sparkSession.close()

  }

}
