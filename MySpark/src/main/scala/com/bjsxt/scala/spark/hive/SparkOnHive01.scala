package com.bjsxt.scala.spark.hive


import org.apache.spark.sql.{SaveMode, SparkSession}

/***
  * spark sql 连接 hive 查询数据

  hive-site.xml 修正
  hive.metastore.uris：sparksql 连接到这里，这里是hive的metastore，用于获取hive表; 另一个是禁用metastore的版本检测
  4.开启hive的metastore元数据库
  spark sql想要使用hive的表，还需要hive开启metastore
  hive --service metastore &
  启动后放后台就可以，供spark sql使用

  */
object SparkOnHive01 {

  def main(args: Array[String]): Unit = {
    // 开启支持 hive .master("spark://hadoop001:7077")
    val sparkSession = SparkSession.builder().appName("SparkOnHive").enableHiveSupport().getOrCreate()

    sparkSession.sql("show databases").show(20)
//    sparkSession.sql("use spark ")

    /*
     注: 使用以下 不能保存， 具体原因不太懂
     */
    import sparkSession.implicits._
    // 创建一个dataSets
    val caseClassDS = Seq(Person("Andy",22),Person("Tomcat",12),Person("From",42),Person("Tidy",32)).toDF()
    // option("spark.sql.hive.convertMetastoreParquet", false).
    caseClassDS.write.mode(SaveMode.Append).saveAsTable("kof")

    sparkSession.stop()
  }

}
case class Person(name: String, age: Long)
