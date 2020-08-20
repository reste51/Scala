package com.bjsxt.scala.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
;

object PartitionOperator {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");

    val conf = new SparkConf().setAppName("other operator").setMaster("local");
    val sc = new SparkContext(conf)

    /**
      * repartition & coalesce
      */
    // 第二个参数: 分区数
    /*val rdd = sc.parallelize(List[String]("data1","data2","data3","data4"
      ,"data5","data6","data7","data8","data9"),3)
    */
    /*
      返回每个分区编号 及 数据
      index 为partition的编号， iter是该编号partition下的数据
     */
    /*val rdd1 = rdd.mapPartitionsWithIndex( (index,iter)=>{
      val list = new ListBuffer[String]()

      while(iter.hasNext){
        // 累计当前的数据
        list.+=(s"rdd1 partition=[ $index] , data=[ ${iter.next()}]")
      }
      list.iterator
    })*/
    /*
      repartition 控制rdd的partition 增加或减少, 产生shuffle. 形成宽依赖
      Return a new RDD,use a shuffle to redistribute data.
      调用源码： coalesce(numPartitions, shuffle = true)
      常用于 扩大分区
     */
    /*
    val rdd2 = rdd1.repartition(2).mapPartitionsWithIndex( (index,iter)=>{
      val list = new ListBuffer[String]()

      while(iter.hasNext){
        // 累计当前的数据
        list.+=(s"rdd2 partition=[ $index] , data=[ ${iter.next()}]")
      }
      list.iterator
    })*/

    /*
      coalesce 默认 no shuffle, 窄依赖,  父rdd的partition 对子rdd 一对一
       --> in a narrow dependency
       如果 扩大分区， 则不会生效, 因为 默认不会shuffle
       因此常用于 减少分区
      */
    /*val rdd2 = rdd1.coalesce(4).mapPartitionsWithIndex( (index,iter)=>{
      val list = new ListBuffer[String]()

      while(iter.hasNext){
        // 累计当前的数据
        list.+=(s"rdd2 partition=[ $index] , data=[ ${iter.next()}]")
      }
      list.iterator
    })*/

//    rdd1.foreach(println)
//    rdd2.foreach(println)
//    rdd2.collect().foreach(println)


    /**
      * transform 算子
      * zip & zipWithIndex
      */
    val rdd1 = sc.parallelize(List[String]("zhangsan","lisi","wangwu","liujian"))
    val rdd2 = sc.parallelize(List[String]("100","200","300","400"))
    /*
      Zips this RDD with another one, returning key-value pairs with the first element in each RDD,second element in each RDD
      生成的结果：[ (zhangsan,100),(lisi,200)]
     */
    val zipRetRdd:RDD[(String,String)] = rdd1.zip(rdd2)
//    zipRetRdd .foreach(println)

    /*
    the partition index and then the ordering of items within each partition.
     So the first item in the first partition gets index 0
     */
    val indexRdd:RDD[((String,String),Long)] = zipRetRdd.zipWithIndex()
//    indexRdd.foreach(println)

    /*
      * Group the values for each key in the RDD into a single sequence.
      * result : (aaa,CompactBuffer(10, 101, 110))  (aaa111,CompactBuffer(10))
      */
//    sc.parallelize(List[(String,Int)](("aaa",10),("aaa",101),("aaa",110),("aaa111",10))).groupByKey().foreach(println)

    /*
      action 算子返回一个结果集
     */
//    val reduceRet = sc.parallelize(List[Int](10,20,10,10)).reduce((one,two)=>one+1)
//    println(reduceRet)

    val rdd3 = sc.parallelize(List[(String,Int)](("aaa",10),("aaa",101),("aaa",110),("aaa111",10)))
    /*
      Count the number of elements for each key, collecting the results to a local Map
      result : (aaa:3,aaa111:1)
     */
    rdd3.countByKey().foreach(println)





    sc.stop()

  }
}
