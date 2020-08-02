package com.sxt.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * 注： 需要 配置 域名解析 -- ip  到 hosts 文件中
 * @ClassName HbaseDemo
 * @Description TODO
 * @Author DELL
 * @Date 2020/8/1 22:04
 * @Version 1.0
 **/
public class HbaseDemo {
    private static Configuration configuration = new Configuration();
    // 表的管理类
    private  static HBaseAdmin admin = null;

    // 具体的表数据管理
    private  static HTable hTable = null;

    private static String tableName = "java_tab";

    // init
    static {
        try {
        // 客户端只需连接Zookeeper即可
        configuration.set("hbase.zookeeper.quorum","192.168.240.139");
        admin = new HBaseAdmin(configuration);

        hTable = new HTable(configuration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void  createTable(String tableName, String cfName) throws Exception{
        // 创建 表和 列族
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor colFamily = new HColumnDescriptor(cfName.getBytes());

        table.addFamily(colFamily);

        if(admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        admin.createTable(table);
    }

    /**
     *  list 'test'
         put 'test', 'row1', 'cf:a', 'value1'

         scan 'test'
         get 'test', 'row1'

     * @throws IOException
     */

    public static void insert() throws Exception {
        // 传入 rowKey
        Put put = new Put("222".getBytes());

        // 可批量添加 多个列的值
        put.add("col".getBytes(),"name".getBytes(),"tom".getBytes());
        put.add("col".getBytes(),"age".getBytes(),"23".getBytes());
        put.add("col".getBytes(),"gender".getBytes(),"male".getBytes());

        hTable.put(put);
    }

    public static void getValue() throws IOException {
        // 传入rowkey
        Get get = new Get("222".getBytes());
        // 只返回列族下的某一列数据
        get.addColumn("col".getBytes(),"name".getBytes());
        get.addColumn("col".getBytes(),"age".getBytes());
        get.addColumn("col".getBytes(),"gender".getBytes());
        // 获取 一行记录_ 进而根据列族获取每列数据
        Result result = hTable.get(get);
        Cell cell1 = result.getColumnLatestCell("col".getBytes(),"name".getBytes());
        Cell cell2 = result.getColumnLatestCell("col".getBytes(),"age".getBytes());
        Cell cell3 = result.getColumnLatestCell("col".getBytes(),"gender".getBytes());

        System.out.println(Bytes.toString(CellUtil.cloneValue(cell1)));
        System.out.println(Bytes.toString(CellUtil.cloneValue(cell2)));
        System.out.println(Bytes.toString(CellUtil.cloneValue(cell3)));
    }

    public static void scanTable(){
        Scan scan = new Scan();

    }

    public static void close() throws IOException {
        admin.close();
    }

    public static void main(String[] args) throws Exception {
//        createTable(tableName,"col");

        // 插入数据
//        insert();

        getValue();
        close();

    }



}
