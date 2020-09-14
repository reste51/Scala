package com.bjsxt.scala.spark.sql

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * spark 连接 mysql的某张表, 创建 df 查询
  */
object CreateDfFromMysql {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("CreateDfFromMysql")
      .config("spark.sql.shuffle.partitions",1).getOrCreate()

    // 设置日志级别
    sparkSession.sparkContext.setLogLevel("ERROR")


    //1. Properties
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")

    val mysqlDf = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/test","person",properties)
//    mysqlDf.show(20)

    //2. map
    val mapConfig = Map[String,String](
      "user"->"root",
      "url"->"jdbc:mysql://localhost:3306/test",
      "password"->"123456",
      "dirver"->"com.mysql.jdbc.driver",
      "dbtable"->"person"
    )
    val mapMysqlDf = sparkSession.read.format("jdbc").options(mapConfig).load()
//    mapMysqlDf.show(20)

    //3. 注册临时表 来 join查询
    mysqlDf.createOrReplaceTempView("p1")
    mapMysqlDf.createOrReplaceTempView("p2")

    val joinRetDf = sparkSession.sql(" select p1.name,p2.age from p1 join p2 on p1.id=p2.id ")

    //4. 将查询的结果保存到 mysql表中
    joinRetDf.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/test","joinRet",properties)


    sparkSession.stop()
  }

}
