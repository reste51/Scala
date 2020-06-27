package com.imooc.spark.dataFrame

import org.apache.spark.sql.SparkSession

/***
  * DataFrame基本操作
  */
object DataFrameApp {

  def main(args: Array[String]): Unit = {
    //注：  设置 hadoop 的home目录, 便于访问 winutils 工具
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");

    val session = SparkSession.builder().master("local[2]").appName("DataFrame operator").getOrCreate()

    // 读取json文件 转为 DF (类似 表)
    val peopleDf = session.read.json("E:\\excise\\bigData\\people.json")
    peopleDf.show()

    // 输出表的元数据(字段类型/名称)
    peopleDf.printSchema()

    // 查询某个字段
    peopleDf.select("age").show()
    peopleDf.select(peopleDf.col("name"), (peopleDf.col("age")+10).as("age_10")).show()


    // 根据某个字段的过滤
    peopleDf.filter("age>19").show()
//    peopleDf.filter(peopleDf.col("age")>19).show()

    session.stop()
  }
}
