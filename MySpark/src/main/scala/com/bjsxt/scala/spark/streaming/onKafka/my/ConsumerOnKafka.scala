package com.bjsxt.scala.spark.streaming.onKafka.my

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * 消费 kafka的数据
  */
object ConsumerOnKafka {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("ConsumerOnKafka")
    val ssc = new StreamingContext(conf,Durations.seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.136.131:9092,192.168.136.130:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "console-consumer-31586",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("quickstart-events")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val tupleDstram: DStream[(String, String)] = stream.map(record => (record.key, record.value))
    tupleDstram.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
