package com.bjsxt.scala.spark.hive.learn

import org.apache.spark.sql.SparkSession

/**
  * Spark 访问 Hive的元数据,  创建及 访问表  DDL DML等操作
  * Hive dependencies must also be present on all of the worker nodes, in order to access data stored in Hive.
  * Configuration of Hive is done by placing your hive-site.xml, core-site.xml (for security configuration), and hdfs-site.xml (for HDFS configuration) file in conf/.
  *
  * 注意： Hive元数据的位置   hive-site.xml  hive.metastore.warehouse.dir
  */
object Hive01 {

  case class  Record(key:Int, value: String)

  def main(args: Array[String]): Unit = {
    // 注： 设置了hadoop-home 之后默认会 查找 hdfs 中的文件,读取本地文件路径:  需要使用 file:///E:/xxxx/xxx
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");
    val path = "file:///"+  this.getClass.getClassLoader.getResource("data/kv1.txt").getPath()

    /*
     会访问 core-site.xml， 会读取linux中 hadoop的conf/core-site.xml 中的hdfs ip 及端口号
     hive 元数据的配置路径: hive-site.xml   <name>hive.metastore.warehouse.dir</name>   指的是hive元数据的位置(table databases,serd等)
     warehouseLocation points to the default location for managed databases and tables
     */
    val spark = SparkSession.builder().appName("Spark Hive Example")
//      .config("spark.sql.warehouse.dir",warehouseLocation)      // 指定hive 存储的路径, 一般是hdfs上的路径
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    sql("use example")
    sql(" create table if not exists src(key int, value string)")
    // 加载数据
    sql(s" load data local inpath '$path'into table src")

    // hiveQL
    sql("select * from src").show()
  }
}
