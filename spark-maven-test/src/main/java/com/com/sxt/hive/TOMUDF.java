 package com.com.sxt.hive;

 import org.apache.hadoop.hive.ql.exec.UDF;
 import org.apache.hadoop.io.Text;

 /** 自定义  hive 的 user defined  function
 * @ClassName TOMUDF
 * @Description TODO
 * @Author 50204
 * @Date 2020/7/6 22:04
 * @Version 1.0
 **/
public final class TOMUDF  extends UDF{

     public Text evaluate(final Text text){
         String str = "";
         if(text == null) str = "empty text";

         return  new Text(text.toString() +" my first udf!");
     }
}
