package com.bjsxt.scala.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * spark sql 初步操作
  */
object First {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");

    val sparkSession = SparkSession.builder().master("local").appName("spark sql first").getOrCreate()
    val path = "./data/json"
    // 读取Json的两种方式
    val df: DataFrame =sparkSession.read.json(path)
//    val df: DataFrame = sparkSession.read.format("json").load(path)
    /*
    root    字段类型自动推断
    |-- age: long (nullable = true)
    |-- name: string (nullable = true)
     */
    df.printSchema()
    df.show()

    // 注册视图 _使用sql查询
//    df.registerTempTable("first")  // 1.6 的语法
    df.createOrReplaceTempView("first") // 不能跨session 访问该表
    df.createGlobalTempView("firstGlobal") // 跨session访问

    // sql查询
    sparkSession.sql("select * from first t where t.age > 18").show(30)

    val newSession: SparkSession = sparkSession.newSession()
    //注： 访问全局视图，需要加入前缀 global_temp
    newSession.sql("select count(*) as consCount from global_temp.firstGlobal t where t.age > 18").show(30)


    sparkSession.stop()
  }
}
