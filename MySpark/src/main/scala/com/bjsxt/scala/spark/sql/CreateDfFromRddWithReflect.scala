package com.bjsxt.scala.spark.sql

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * 使用反射的方式
  * text -> dataSet -> case class Person() -> DF -> 创建临时表 ->sql 查询
  * dataSet与 rdd类似, 但 2.0+ 推荐使用
  */

// 用于标识rdd的类型
case class Person(id:Int, name:String, age:Int, score:Long)

object CreateDfFromRddWithReflect {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("CreateDfFromRddWithReflect").getOrCreate()

    import session.implicits._
    // 创建 dataSet
    val dataSet: Dataset[String] = session.read.textFile("./data/people.txt")
    // 创建一个rdd
//    val rdd: RDD[String] = session.sparkContext.textFile("./data/people.txt")

    // 转为 Person类型的dataSet
    val pDs: Dataset[Person] = dataSet.map(row => {
      //1,zhangsan,18,100
      val arr: Array[String] = row.split(",")
      Person(arr(0).toInt, arr(1), arr(2).toInt, arr(3).toLong)
    })

    //转为 Df
//    pDs.toDF().show(20)
    val pDf = pDs.toDF()
    //创建临时表
    pDf.createOrReplaceTempView("person")

    //sql查询
    session.sql(" select name,age from person where age > 18").show(29)

    session.stop()

  }

}

