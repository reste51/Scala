package com.bjsxt.scala.spark.streaming.learn

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Let’s say we want to count the number of words in text data received from a data server listening on a TCP socket.
  *  StreamingContext is the main entry point for all streaming functionality.
  *
  *  Points to remember:
    Once a context has been started, no new streaming computations can be set up or added to it.
    Once a context has been stopped, it cannot be restarted.
    Only one StreamingContext can be active in a JVM at the same time.
    stop() on StreamingContext also stops the SparkContext. To stop only the StreamingContext, set the optional parameter of stop() called stopSparkContext to false.
    A SparkContext can be re-used to create multiple StreamingContexts, as long as the previous StreamingContext is stopped (without stopping the SparkContext) before the next StreamingContext is created.
  */
object QuickStart {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");
    /*
      We create a local StreamingContext with two execution threads, and a batch interval of 1 second.
       The master requires 2 cores to prevent from a starvation scenario( 一个接收数据， 一个处理数据) .
     */
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    /*
     内部会创建 SparkContext实例，通过 ssc.sparkContext 访问
      Note that this internally creates a SparkContext (starting point of all Spark functionality) which can be accessed as ssc.sparkContext.
      */
    val ssc = new StreamingContext(conf,Seconds(1))
//    ssc.sparkContext.textFile()

//    A StreamingContext object can also be created from an existing SparkContext object.
//    val ssc = new StreamingContext(sc, Seconds(1))


    /*
      create a DStream that represents streaming data from a TCP source
      This lines DStream represents the stream of data that will be received from the data server
     */
    val lines = ssc.socketTextStream("nodeM",9999)

    /*
      Each record in this DStream is a line of text. Next, we want to split the lines by space characters into words.
      flatMap is a one-to-many DStream operation that creates a new DStream by generating multiple
        new records from each record in the source DStream.
       flatMap 是输入一条  生成多条 数据, 从而生成一个新的 DS。
       each line will be split into multiple words and the stream of words is represented as the words DStream.
      */
    val words = lines.flatMap(_.split(" "))

    /*
      Next, we want to count these words. count each words in each batch
      mapped (one-to-one transformation) to a DStream of (word, 1) pairs,which is then reduced to get the frequency of words in each batch of data. .
      */
    val pairs = words.map(word=> (word,1))
    val wordCounts = pairs.reduceByKey(_+_)

    // Print the first ten elements of each RDD generated in this DStream by  every second,
    wordCounts.print()

    ssc.start()    //start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

  }
}
