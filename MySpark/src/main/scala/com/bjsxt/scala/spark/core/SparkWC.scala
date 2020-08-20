package com.bjsxt.scala.spark.core

import org.apache.spark.{SparkConf, SparkContext}
;

/**
  * spark wordcount
  */
object SparkWC {
  def main(args: Array[String]): Unit = {

    //注：  设置 hadoop 的home目录, 便于访问 winutils 工具
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");

    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.textFile("./data/words").flatMap( _.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
    sc.stop()




//    //conf 可以设置SparkApplication 的名称，设置Spark 运行的模式
//    val conf = new SparkConf()
//    conf.setAppName("wordcount")
//    conf.setMaster("local")
//    //SparkContext 是通往spark 集群的唯一通道
//    val sc = new SparkContext(conf)
//
//    val lines: RDD[String] = sc.textFile("./data/words")
//    val words: RDD[String] = lines.flatMap(line => {
//      line.split(" ")
//    })
//    val pairWords: RDD[(String, Int)] = words.map(word=>{new Tuple2(word,1)})
//    val result: RDD[(String, Int)] = pairWords.reduceByKey((v1:Int, v2:Int)=>{v1+v2})
//    result.foreach(one=>{
//      println(one)
//    })

  }
}
