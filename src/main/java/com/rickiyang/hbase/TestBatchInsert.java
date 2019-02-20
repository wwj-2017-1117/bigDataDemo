package com.rickiyang.hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Author yangyue
 * @Date Created in 下午2:57 2018/10/19
 * @Modified by:
 * @Description:  百万数据写入
 **/
public class TestBatchInsert {

    public static void main(String[] args) {
        try {
            long start = System.currentTimeMillis();
            Configuration conf = HBaseConfiguration.create();
            Connection conn = ConnectionFactory.createConnection(conf);
            TableName tname = TableName.valueOf("ns1:t1");
            HTable table = (HTable)conn.getTable(tname);
            //不要自动清理缓冲区
            table.setAutoFlush(false);

            for(int i = 4 ; i < 1000000 ; i ++){
                Put put = new Put(Bytes.toBytes("row" + i)) ;
                //关闭写前日志
                put.setWriteToWAL(false);
                put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("id"),Bytes.toBytes(i));
                put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("name"),Bytes.toBytes("tom" + i));
                put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age"),Bytes.toBytes(i % 100));
                table.put(put);

                if(i % 2000 == 0){
                    table.flushCommits();
                }
            }
            //
            table.flushCommits();
            System.out.println(System.currentTimeMillis() - start );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
