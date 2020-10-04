package com.bjsxt.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * 窗口操作- 不是从头到尾都是累加， 而是可以指定一个长度和周期的统计,
  *   例如： 每隔 5s 统计过去 1分钟的数据处理。
  *
  * 窗口长度_ window length：1分钟则是，处理量
  * 窗口滑动间隔_ slide interval：处理数据与上一批次处理的间隔
  *
  * 窗口优化则是 中间结果不用统计， 增加 新进的batch, 减去 滑出的batch；
  *   此机制需要使用 checkpoint 来保存上批次数据
  */
object WindowOperator {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");

    val sparkConf = new SparkConf().setAppName("WindowOperator").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Durations.seconds(5))

    ssc.sparkContext.setLogLevel("ERROR")

    // word count
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.136.131",9999)
    val words: DStream[String] = lines.flatMap(line=>{line.split(" ")})
    val pairWords: DStream[(String, Int)] = words.map(word=>{(word,1)})

    /**
      窗口操作普通的机制，
      滑动间隔和窗口长度必须是 batchInterval 整数倍

      操作的api : window,  reduceByKeyAndWindow
      */
    // 根据 slideInterval 处理返回 length的batch的 由rdd封装的DStream
//    val ds : DStream[(String,Int)] = pairWords.window(Durations.seconds(15),Durations.seconds(5))

    // 参数2：length,  参数3： slide interval
//      val retStream: DStream[(String, Int)] = pairWords.reduceByKeyAndWindow((v1:Int, v2:Int)=>{v1+v2},
//          Durations.seconds(10),Durations.seconds(5))

    /*
    窗口优化则是 中间结果不用统计， 增加 新进的batch (param 1), 减去 滑出的batch(param 2)
     配合checkpoint 来保存上一批次的数据
     */
    ssc.checkpoint("./data/windowCheckpoint")
    val retStream: DStream[(String, Int)] = pairWords.reduceByKeyAndWindow(
      (v1: Int, v2: Int) => {
        v1 + v2
      },
      (v1: Int, v2: Int) => {
        v1 - v2
      },
      Durations.seconds(10),
      Durations.seconds(5))

    retStream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
