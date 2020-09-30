package com.bjsxt.scala.spark.streaming

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/***
  * SparkStreaming 初始
  */
object StreamingTest01 {
  def main(args: Array[String]): Unit = {
    /*
      至少使用 2个线程, 1监控来数据; 1来处理数据
        spark.master should be set as local[n], n > 1 in local mode if you have receivers to get data,
        otherwise Spark jobs will not get resources to process the received data.
      */
    val sparkConf = new SparkConf().setAppName("StreamingTest01").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    // 设置Batch interval 为 5s
    val ssc = new StreamingContext(sc,Durations.seconds(5))
    val inputDataBatch: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.136.131",9999)

    // wordCount 计算模式, DStream 也可以使用RDD的算子，做了一层封装
    val wordCount = inputDataBatch.flatMap(_.split(" ")).
      map(word=>(word,1)).reduceByKey(_+_)
    // output operator可以触发 transformation算子
    wordCount.print()

    ssc.start()   //开启计算
    ssc.awaitTermination()  // 等待计算完成
    ssc.stop(true)
  }
}
