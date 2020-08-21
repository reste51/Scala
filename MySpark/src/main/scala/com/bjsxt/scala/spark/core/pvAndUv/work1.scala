package com.bjsxt.scala.spark.core.pvAndUv

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1.pv pageview ( 同一个人访问多次_不去重)
    2.uv unique vistor(一个人访问多次，去重计数为1)
    3.每个网址 访问量top3地区 和对应的人数
      www.baidu.com 北京 2000
                     河北 1800
               辽宁 1000
	www.taobao.com....
  */
object work1 {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");
    val conf = new SparkConf().setMaster("local").setAppName("pvAndUv Task")
    val  sc = new SparkContext(conf)

    val  rdd = sc.textFile("./data/pvuvdata")

    /***********************PV 的操作****************************/
    // map => [],[],[] => (webSite,1)  => reduceByKey => sortBy (value )
    val msgRdd = rdd.map(line=> line.split("\t")).map(arr => {
        //case class Message(ip:String, address:String, date:String,timestamp:String, userid: String, webSite:String, action:String)
//      (arr(5),Message(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6)))
      (arr(5),1)
    })
//    msgRdd.foreach(println)
    /*
      sortBy 第一个参数: 指定排序的值为 _2 value
      (www.taobao.com,18715)
      (www.suning.com,18653)
      (www.gome.com.cn,18635)
      (www.dangdang.com,18565)
      (www.baidu.com,18534)
      (www.mi.com,18456)
      (www.jd.com,18418)
     */
//    msgRdd.reduceByKey((one,two)=>one+two).sortBy(_._2,false).foreach(println)
//    println("=====================")
//    msgRdd.countByKey().foreach(println)

    ///

    /***********************PV 的操作****************************/
    /*
       map => [],[],[] => (webSite_ip,1)  => distinct (去重 ip+website)
            => map (website,1)
            => reduceByKey(根据相同的网站累计数) => sortBy (value )

       distinct:      map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
       result:
        (www.taobao.com,15725)
        (www.suning.com,15724)
        (www.dangdang.com,15683)
        (www.gome.com.cn,15640)
        (www.baidu.com,15607)
        (www.mi.com,15558)
        (www.jd.com,15532)
      */
    rdd.map(_.split("\t")).map(arr => (arr(5)+"_"+arr(0),1)).distinct()
      .map(tuple=> (tuple._1.split("_")(0),1))
        .reduceByKey(_+_).sortBy(tuple => tuple._2,false).foreach(println)



    sc.stop()
  }
}

//val content = ip + "\t" + address + "\t" + date + "\t" + timestamp + "\t" + userid + "\t" + webSite + "\t" + action
//170.201.151.133	四川	2020-08-20	1597923327428	2505024611792698201	www.jd.com	Regist
case class Message(ip:String, address:String, date:String,timestamp:String, userid: String, webSite:String, action:String)
