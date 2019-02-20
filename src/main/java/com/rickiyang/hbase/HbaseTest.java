package com.rickiyang.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author yangyue
 * @Date Created in 下午5:58 2019/1/19
 * @Modified by:
 * @Description:
 **/
public class HbaseTest {

        static Configuration conf = null;
        static HBaseAdmin admin = null;
        static Connection connection = null;
        static Table table = null;


        @Before
        public void init() throws IOException {
            //创建conf对象
            conf = HBaseConfiguration.create();
            //通过连接工厂创建连接对象
            connection = ConnectionFactory.createConnection(conf);
            //通过连接查询tableName对象
            TableName tname = TableName.valueOf("ns1:t1");
            //获得table
            table = connection.getTable(tname);
        }



        @Test
        public void testGetMultipleRecords() throws IOException {
            Scan scan = new Scan();
            // 这里的Row设置的是RowKey！而不是行号
            scan.withStartRow(Bytes.toBytes(1)).withStopRow(Bytes.toBytes(10));
            //设置扫描缓存
            scan.setCaching(1000);
            ResultScanner scanner = table.getScanner(scan);
            printResultScanner(scanner);
        }


        public void printResultScanner(ResultScanner scanner) {
            for (Result row : scanner) {
                // Hbase中一个RowKey会包含[1,n]条记录，所以需要循环
                System.out.println("\n RowKey:" + String.valueOf(Bytes.toInt(row.getRow())));
                printRow(row);
            }
        }

        public void printRow(Result row) {
            for (Cell cell : row.rawCells()) {
                System.out.print(String.valueOf(Bytes.toInt(cell.getRow())) + " ");
                System.out.print(new String(cell.getFamily()) + ":");
                System.out.print(new String(cell.getQualifier()) + " = ");
                System.out.print(new String(cell.getValue()));
                System.out.print("   Timestamp = " + cell.getTimestamp());
            }
        }

}
