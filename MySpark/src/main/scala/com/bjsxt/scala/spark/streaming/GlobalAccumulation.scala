package com.bjsxt.scala.spark.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * 通过 checkpoint 来实现全局累加, 每个batch 都可以获取上次的batch数据
  *
  * 内存--> 硬盘保存的频率； 以batchInterval 10s为标杆, 小于10s则为10s,  大于10s 则使用 当前的batchInterval
  *
  * UpdateStateByKey 根据key更新状态
  *   1、为Spark Streaming中每一个Key维护一份state状态，state类型可以是任意类型的， 可以是一个自定义的对象，那么更新函数也可以是自定义的。
  *   2、通过更新函数对该key的状态不断更新，对于每个新的batch而言，Spark Streaming会在使用updateStateByKey的时候为已经存在的key进行state的状态更新
  *
  * 注: 此checkpoint 从core-site.xml 读取hdfs配置, 会保存到 hdfs: /user/50204/data/myCheckpoint 中,
  * 具体为啥不能保存本地路径不太清楚， 因此需要开启hadoop服务。
  */
object GlobalAccumulation {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("GlobalAccumulation")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Durations.seconds(5))
    // 提高日志级别
//    ssc.sparkContext.setLogLevel("ERROR")

    // 接收 socket 端传递的  数据流,  经过 一组rdd 封装后的DStream
    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.136.131", 9999)
    // word count
    val pairWord: DStream[(String, Int)] = dStream.flatMap(_.split(" ")).map((_,1))


    /*
      注：在调用updateStateByKey 之前需要使用checkpoint,否则会：
        The checkpoint directory has not been set. Please set it by StreamingContext.checkpoint().

         * 根据key更新状态，需要设置 checkpoint来保存状态
      * 默认key的状态在内存中 有一份，在checkpoint目录中有一份。
      *
      *    多久会将内存中的数据（每一个key所对应的状态）写入到磁盘上一份呢？
      * 	      如果你的batchInterval小于10s  那么10s会将内存中的数据写入到磁盘一份
      * 	      如果bacthInterval 大于10s，那么就以bacthInterval为准
      *
      *    这样做是为了防止频繁的写HDFS
     */
    ssc.checkpoint("./data/myCheckpoint")
//    ssc.sparkContext.setCheckpointDir("./data/myCheckpoint")

    /*
      此时不能用 .reduceByKey(_+_)  该方法只会累加本次的batch

      * currentValues :当前批次某个相同的 key 对应所有的value 组成的一个集合
      * preValue : 以往批次当前key 对应的总状态值
     */
    val totalRet = pairWord.updateStateByKey((currentValues: Seq[Int], preValue: Option[Int])=>{
      var count = 0
      // 累加当前 batch相同的值
      currentValues.foreach(value=>count+=value)
      // 累加上批次的值
      if(!preValue.isEmpty) count+=preValue.get

      Option(count)
    })

    totalRet.print()

    // 开启 streaming 流处理
    ssc.start()
    ssc.awaitTermination()
  }

}
