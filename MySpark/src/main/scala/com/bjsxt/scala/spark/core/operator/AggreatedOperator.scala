package com.bjsxt.scala.spark.core.operator

import org.apache.spark.sql.SparkSession

/**
  * aggregate() 函数的返回类型不需要和 RDD 中的元素类型一致，所以在使用时，需要提供所期待的返回类型的初始值，然后通过一个函数把 RDD 中的元素累加起来放入累加器。
*考虑到每个结点都是在本地进行累加的，所以最终还需要提供第二个函数来将累加器两两合并。
*aggregate(zero)(seqOp,combOp) 函数首先使用 seqOp 操作聚合各分区中的元素，然后再使用 combOp 操作把所有分区的聚合结果再次聚合，两个操作的初始值都是 zero。
 **
 seqOp 的操作是遍历分区中的所有元素 T，第一个 T 跟 zero 做操作，结果再作为与第二个 T 做操作的 zero，直到遍历完整个分区。
*combOp 操作是把各分区聚合的结果再聚合。aggregate() 函数会返回一个跟 RDD 不同类型的值。因此，需要 seqOp 操作来把分区中的元素 T 合并成一个 U，以及 combOp 操作把所有 U 聚合。
  *
  */
object AggregatedOperator {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("AggregatedOperator").getOrCreate()
    val sc = spark.sparkContext

    /*
    定义一个初始值 (0,0)，即所期待的返回类型的初始值。
    代码 (acc,value) => (acc._1 + value,acc._2 + 1) 中的 value 是函数定义里面的 T，这里是 List 里面的元素。acc._1 + value，acc._2 + 1 的过程如下。
(0+1,0+1)→(1+2,1+1)→(3+3,2+1)→(6+4,3+1)，结果为(10,4)。

实际的 Spark 执行过程是分布式计算，可能会把 List 分成多个分区，假如是两个：p1(1,2) 和 p2(3,4)。

经过计算，各分区的结果分别为 (3,2) 和 (7,2)。这样，执行 (acc1,acc2) => (acc1._1 + acc2._2,acc1._2 + acc2._2) 的结果就是 (3+7,2+2)，即 (10,4)，然后可计算平均值。
     */
    val rdd = List (1,2,3,4)
    val input = sc.parallelize(rdd)
    val result = input.aggregate((0,0))(
      (acc,value) => (acc._1 + value,acc._2 + 1),
      (acc1,acc2) => (acc1._1 + acc2._1,acc1._2 + acc2._2)
    )
//    result:(Int,Int) = (10,4)
//    val avg = result._1 / result._2
//    avg:Int = 2.5
   print(result)

    spark.stop()
  }
}
