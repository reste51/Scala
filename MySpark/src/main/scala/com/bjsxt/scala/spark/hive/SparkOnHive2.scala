package com.bjsxt.scala.spark.hive

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * 使用sql访问 hive， 并创建 表 和 加载数据/  存储表。


    hive-site.xml 修正
  hive.metastore.uris：sparksql 连接到这里，这里是hive的metastore，用于获取hive表; 另一个是禁用metastore的版本检测

4.开启hive的metastore元数据库
  spark sql想要使用hive的表，还需要hive开启metastore
  hive --service metastore &
  启动后放后台就可以，供spark sql使用

  注： 使用时需 使用 spark-submit 提交 standalone 模式， 不用local会报错

  */
object SparkOnHive2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkOnHive2").enableHiveSupport().getOrCreate()

    // 隐式导入

    spark.sql("create table if not exists src(key int, value string)")
    spark.sql("load data local inpath '/home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/kv1.txt' into table src")

    // Queries are expressed in HiveQL
    spark.sql("select * from src").show(20)

    // Aggregation queries are also supported.
    spark.sql("select count(*) from src").show()

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    import  spark.implicits._

    // The items in DaraFrames are of type Row, which allows you to access each column by ordinal.
    val stringDS = sqlDF.map{
      case Row(key:Int, value:String)=> s"the key is $key, and the value is $value"
    }
    stringDS.show(200,false)
    //save table
    stringDS.write.mode(SaveMode.Overwrite).saveAsTable("src_ret")


    // You can also use DataFrames to create temporary views within a SparkSession.
//    val recordsDF= spark.createDataFrame((1 to 100).map(i=> Record)


  }
}
