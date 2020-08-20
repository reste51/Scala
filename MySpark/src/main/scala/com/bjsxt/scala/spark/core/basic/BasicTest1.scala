package com.bjsxt.scala.spark.basic

/**
  * scala 基础语法学习
  */
object BasicTest1 {
  def main(args: Array[String]): Unit = {
    /*
    Array 为 不可变的集合_  默认调用Array的 apply函数
     */
//    println(func(5))
    val arr =  Array[String]("1","23")

    // 初始化2个元素的集合, 默认值为0
    val arr2 = new Array[Int](2)
//    arr2(0)=100
//    arr2(1)=200
    arr2.foreach(println)


    //Map  默认调用Map的 apply函数
    val map = Map[String,Int]("a"->1,"c"->2,"b"->1,"d"->4,("oo",100),("ooq",10))
    /*for(item <- map){
      println(s"${item._1} --- ${item._2} ===== $item")
    }*/
    map.foreach(one => println(s"${one._1} = ${one._2}"))
    map.get("a")


  }


  /*
   递归方法
   显示声明函数 返回类型
   */
  def func(num:Int):Int = {
    if(num ==1){
      1
    }else{
      num * func(num-1)
    }
  }

    val xx = (a: Int, b: Int) => a + b

}
