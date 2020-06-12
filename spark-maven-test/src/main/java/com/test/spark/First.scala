import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Yezhiwei
  */

object First {
  def main (args: Array[String]){

    //注：  设置 hadoop 的home目录, 便于访问 winutils 工具
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val conf = new SparkConf().setMaster("spark://192.168.136.130:7077").setAppName("TestMaven")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("hdfs://192.168.136.130/mp/hello.txt")
    // Number of items in this RDD
    print("总数为：  "+rdd.count())

    // First item in this RDD
//    val firstText = rdd.first()
//    print("第一个值： " + firstText)

    /**
      * We will use the filter transformation to return a new RDD with a subset of the items in the file.
      *  How many lines contain "hello"?
      */
//    val count = rdd.filter(line => line.contains("hello")).count()
//    print("How many lines contain \"hello\"?" + count)
    sc.stop()
  }
}