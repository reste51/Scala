import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Yezhiwei
  */

object WordCount {
  def main (args: Array[String]){

    //注：  设置 hadoop 的home目录, 便于访问 winutils 工具
    System.setProperty("hadoop.home.dir", "D:\\program_files\\winutils\\winutils-master\\hadoop-2.6.3");
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val conf = new SparkConf().setMaster("spark://192.168.136.129:7077").setAppName("WC")
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://192.168.136.129/mp/hello.txt")
    print(input.count())
//    val pythonLines = input.filter(line => line.contains("Python"))
//    val words = pythonLines.flatMap(line => line.split(" "))
//    val counts = words.map(word => (word, 1)).reduceByKey(_ + _)
//
//    counts.saveAsTextFile("outputFile")
    sc.stop()
  }
}