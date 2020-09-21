package com.bjsxt.scala.spark.hive


import org.apache.spark.sql.{SaveMode, SparkSession}

/***
  * spark sql 连接 hive 查询数据
  */
object SparkOnHive01 {

  def main(args: Array[String]): Unit = {
    // 开启支持 hive .master("spark://hadoop001:7077")
    val sparkSession = SparkSession.builder().appName("SparkOnHive").enableHiveSupport().getOrCreate()

    sparkSession.sql("show databases").show(20)
//    sparkSession.sql("use spark ")
    import sparkSession.implicits._
    // 创建一个dataSets
    val caseClassDS = Seq(Person("Andy",22),Person("Tomcat",12),Person("From",42),Person("Tidy",32)).toDF()
    caseClassDS.write.option("spark.sql.hive.convertMetastoreParquet", false).mode(SaveMode.Append).saveAsTable("kof")


    sparkSession.stop()
  }

}
case class Person(name: String, age: Long)
