package com.bjsxt.scala.spark.sql.learn

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Registering a DataFrame as a temporary view allows you to run SQL queries over its data.
  * loading and saving data using the Spark Data Sources
  */
object SparkDataSources {
  val rootPath = "file:///E:\\excise\\scala_test\\data\\example";

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");

    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local")
      .config("spark.some.config.option", "some-value").getOrCreate()

    runBasicDataSouceExample(spark)

  }

  // 基础操作
  def runBasicDataSouceExample(spark: SparkSession): Unit = {
    val usersDf = spark.read.load(s"$rootPath/users.parquet")
//    usersDf.show()

    // 取指定的字段保存
//    usersDf.select($"name", $"favorite_color").write.save(s"$rootPath/out/nameAndColor.parquet")

    // 格式转化存储: json--> txt
//    spark.read.json(s"$rootPath/people.json").select("name").write.text(s"$rootPath/out/peopleTxt.text")

    // 读取csv 格式, 配置  分隔符;  显示 字段名，inferSchema 系统推导编码格式
    val csvDf = spark.read.format("csv")
      .option("sep",";").option("header","true").option("inferSchema","true")
        .load( s"$rootPath/people.csv")
//    csvDf.show()
    csvDf.createOrReplaceTempView("csvView")

    spark.sql(" select * from  csvView where age> 30 ").write.mode(SaveMode.Overwrite).saveAsTable("csvRet")

    spark.sql(" select * from csvRet").show()

    // 删除的保存表
    spark.sql(" drop table csvRet")

//    spark.sql(" select * from csvRet")
  }
}
