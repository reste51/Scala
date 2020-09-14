package com.bjsxt.scala.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 演示操作 嵌套的json 及 属性为 jsonArray的情况
  */
object NestJsonDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");
    val session: SparkSession = SparkSession.builder().appName("NestJsonDemo").master("local").getOrCreate()
    // 读取文件
    val df: DataFrame = session.read.json("./data/NestJsonFile")
//    df.show()

    // 注册为表使用sql查询
    df.createOrReplaceTempView("info")
    // 通过 infos.age  来访问嵌套对象的属性值
    session.sql("select infos.age as age,infos.gender as gender, name ,score in from info where score > 60 ").show(20)


    // jsonArray的操作方式
    val arrDf = session.read.format("json").load("./data/jsonArrayFile")
    arrDf.show(false) // false为显示全部的列值
    arrDf.printSchema()

    // 开启隐式转换, 使用$ 方便获取列
    import  org.apache.spark.sql.functions._
    import session.implicits._
    // explode将数组展开,数组中的每个对象 都作为一行数据展示;  array --> struct 类型
    val transDf = arrDf.select($"name", $"age", explode($"scores")).toDF("name","age","allScore")
//    transDf.show(20,false)
    transDf.createOrReplaceTempView("arrTable")
//    transDf.printSchema()

        session.sql("select name,age,allScore.yuwen,allScore.shuxue,allScore.yingyu from arrTable").show(20,false)


    session.stop()

  }
}
