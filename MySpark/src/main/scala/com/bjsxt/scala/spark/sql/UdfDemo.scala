package com.bjsxt.scala.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * 用户自定义函数
  */
object UdfDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("UdfDemo").master("local").getOrCreate()

    val list = List[String]("zhangsan", "lisi", "wangwu","lidian")
    //隐士转化 使用toDF函数
    import sparkSession.implicits._
    val nameDf = list.toDF("name")

//    nameDf.show(20)
    nameDf.createOrReplaceTempView("nameT")

    // 注册UDF函数 LEN
    sparkSession.udf.register("LEN",(item:String)=>{
      item.length
    })
    sparkSession.sql("select name,LEN(name) from nameT").show(20)

    sparkSession.stop()
  }
}
