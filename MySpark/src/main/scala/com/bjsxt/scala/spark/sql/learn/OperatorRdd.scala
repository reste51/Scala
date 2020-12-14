package com.bjsxt.scala.spark.sql.learn

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * rdd转为 DF
  * 1.case class - use reflection
  * 2.编程式, StructType
  *
  * 注:
  * 1、启用隐式转换时，需要在 main 函数中自行创建 SparkSession 对象，然后使用该对象来启用隐式转换，而非在 object 对象之前启用。
  * 2、case class 类的声明需要放在 main 函数之前。
  */
object OperatorRdd {
  case class Person(name:String, age:Integer)
  val rootPath = "file:///E:\\excise\\scala_test\\data\\example";

  /**
    * reflection-  通过case class 来指定DF的 schema
    * @param spark
    */
  def rddByReflection(spark: SparkSession): Unit = {
    import spark.implicits._
    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDf = spark.sparkContext.textFile(s"$rootPath/people.txt").map(_.split(","))
      .map(attrArr=> Person(attrArr(0),attrArr(1).trim.toInt)).toDF()

    peopleDf.createOrReplaceTempView("people")

    val teenagersDf = spark.sql(" select age,name from people where age between 13 and 19 ")
    teenagersDf .show()

    // The columns of a row in the result can be accessed by field index
    teenagersDf.map(row => "Name: "+row(1)).show()

    // or by field name
    teenagersDf.map(row=> "Age : "+ row.getAs("age")).show()
  }

  /**
    * 编程式
    * StructField ,StructType 指定 schema
    * @param spark
    */
  def programSpecifySchema(spark: SparkSession):Unit={
    import spark.implicits._
    // Create an rdd
    val peopleRDD = spark.sparkContext.textFile(s"$rootPath/people.txt")

    //The schema is encoded in a string
    val schemaString = "name age"

    //generate the schema based on the string of schema
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName,StringType,nullable = true))
    val schema: StructType = StructType(fields)

    // RDD -> Rows
    val rowRdd: RDD[Row] = peopleRDD.map(_.split(",")).map(attrArr => Row(attrArr(0),attrArr(1).trim))

    // Apply the schema to the RDD
    val df = spark.createDataFrame(rowRdd,schema)
    df.createOrReplaceTempView("people")

    val resultDf = spark.sql(" select name from people where age > 20")
    // The results of SQL queries are DataFrames and support all the normal RDD operations
    resultDf.map(row=>"name is : " + row(0)).show()


  }

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");

    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local")
      .config("spark.some.config.option", "some-value").getOrCreate()

//    rddByReflection(spark)
    programSpecifySchema(spark)

  }
}
