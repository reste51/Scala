package com.imooc.spark

import java.sql.DriverManager

/**
  * 通过 Jdbc的方式连接 ThriftServer
  */
object SparkSqlThriftServerApp {
  def main(args: Array[String]): Unit = {

    // 注: 加载 hive的jdbc驱动
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    // 连接 spark的 thriftServer
    val connection = DriverManager.getConnection("jdbc:hive2://192.168.240.138:10000","hadoop","hadoop")

    // 操作sql
    val pre = connection.prepareStatement(" select empno,ename,job from emp")
    val resultSet = pre.executeQuery()

    while (resultSet.next()){
      println("empno:" + resultSet.getInt("empno") +", ename : "
        + resultSet.getString("ename") + ", job : " + resultSet.getString("job"))
    }
    resultSet.close()
    pre.close()
    connection.close()

  }
}
