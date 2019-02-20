package com.rickiyang.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * @Author yangyue
 * @Date Created in 下午7:57 2018/10/15
 * @Modified by:
 * @Description:
 **/
public class HbaseApiTest {

    static Configuration conf = null;
    static HBaseAdmin admin = null;
    static Connection connection = null;
    static Table table = null;


    @Before
    public void init() throws IOException
    {
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
    public void put() throws Exception {
        //通过bytes工具类创建字节数组(将字符串)
        byte[] rowid = Bytes.toBytes("row3");

        //创建put对象
        Put put = new Put(rowid);

        byte[] f1 = Bytes.toBytes("f1");
        byte[] id = Bytes.toBytes("id") ;
        byte[] value = Bytes.toBytes(102);
        put.addColumn(f1,id,value);

        //执行插入
        table.put(put);
    }

    @Test
    public void get() throws Exception {
        //通过bytes工具类创建字节数组(将字符串)
        byte[] rowid = Bytes.toBytes("row3");
        Get get = new Get(Bytes.toBytes("row3"));
        Result r = table.get(get);
        byte[] idvalue = r.getValue(Bytes.toBytes("f1"),Bytes.toBytes("id"));
        System.out.println(Bytes.toInt(idvalue));
    }

    @Test
    public void testDeleteTable() throws IOException
    {
        admin.disableTable("test");
        admin.deleteTable("test");
    }
    @Test
    public void testInsertSingleRecord() throws IOException
    {
        Put rk1 = new Put(Bytes.toBytes("rk1"));
        rk1.addColumn(Bytes.toBytes("my_cf1"), Bytes.toBytes("name"), Bytes.toBytes("NikoBelic"));
        table.put(rk1);
    }




    @Test
    public void testInsertMultipleRecords() throws IOException
    {
        List<Put> rowKeyList = new ArrayList<>();
        for (int i = 0; i < 10; i++)
        {
            Put rk = new Put(Bytes.toBytes(i));
            rk.addColumn(Bytes.toBytes("my_cf1"), Bytes.toBytes("name"), Bytes.toBytes("Name" + i));
            rk.addColumn(Bytes.toBytes("my_cf2"), Bytes.toBytes("age"), Bytes.toBytes("Age" + i));
            rowKeyList.add(rk);
        }
        table.put(rowKeyList);
    }
    @Test
    public void testDeleteRecord() throws IOException
    {
        Delete delete = new Delete(Bytes.toBytes(5));
        table.delete(delete);
    }
    @Test
    public void testGetSingleRecord() throws IOException
    {
        Get get = new Get(Bytes.toBytes(4));
        Result result = table.get(get);
        printRow(result);
    }
    @Test
    public void testGetMultipleRecords() throws IOException
    {
        Scan scan = new Scan();
        // 这里的Row设置的是RowKey！而不是行号
        scan.setStartRow(Bytes.toBytes(1));
        scan.setStopRow(Bytes.toBytes(10));
        //scan.setStartRow(Bytes.toBytes("Shit"));
        //scan.setStopRow(Bytes.toBytes("Shit2"));
        ResultScanner scanner = table.getScanner(scan);
        printResultScanner(scanner);
    }
    @Test
    public void testFilter() throws IOException
    {
        /*
        FilterList 代表一个过滤器列表，可以添加多个过滤器进行查询，多个过滤器之间的关系有：
        与关系（符合所有）：FilterList.Operator.MUST_PASS_ALL
        或关系（符合任一）：FilterList.Operator.MUST_PASS_ONE
         过滤器的种类：
            SingleColumnValueFilter - 列值过滤器
                - 过滤列值的相等、不等、范围等
                - 注意：如果过滤器过滤的列在数据表中有的行中不存在，那么这个过滤器对此行无法过滤。
            ColumnPrefixFilter - 列名前缀过滤器
                - 过滤指定前缀的列名
            MultipleColumnPrefixFilter - 多个列名前缀过滤器
                - 过滤多个指定前缀的列名
            RowFilter - rowKey过滤器
                - 通过正则，过滤rowKey值。
                - 通常根据rowkey来指定范围时，使用scan扫描器的StartRow和StopRow方法比较好。
         */
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        Scan s1 = new Scan();
        // 1. 列值过滤器
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes("my_cf1"), Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("Name1"));
        // 2. 列名前缀过滤器
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("ad"));
        // 3. 多个列值前缀过滤器
        byte[][] prefixes = new byte[][]{Bytes.toBytes("na"), Bytes.toBytes("ad")};
        MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(prefixes);
        // 4. RowKey过滤器
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^Shit"));
        // 若设置，则只返回指定的cell，同一行中的其他cell不返回
        //s1.addColumn(Bytes.toBytes("my_cf1"), Bytes.toBytes("name"));
        //s1.addColumn(Bytes.toBytes("my_cf2"), Bytes.toBytes("age"));
        // 设置过滤器
        //filterList.addFilter(singleColumnValueFilter);
        //filterList.addFilter(columnPrefixFilter);
        //filterList.addFilter(multipleColumnPrefixFilter);
        filterList.addFilter(rowFilter);
        s1.setFilter(filterList);
        ResultScanner scanner = table.getScanner(s1);
        printResultScanner(scanner);
    }
    public void printResultScanner(ResultScanner scanner)
    {
        for (Result row : scanner)
        {
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
            System.out.println("");
       }
    }



    @Test
    public void testScan() throws IOException {
        Connection conn = ConnectionFactory.createConnection();
        Table table = conn.getTable(TableName.valueOf("student"));
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("223110")).withStopRow(Bytes.toBytes("225019"));
        scan.addFamily(Bytes.toBytes("base_info"));
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> results = scanner.iterator();
        while (results.hasNext()){
            Result r = results.next();
            String rowId = Bytes.toString(r.getRow());
            Cell cId = r.getColumnLatestCell(Bytes.toBytes("base_info"),Bytes.toBytes("id"));
            Cell cName = r.getColumnLatestCell(Bytes.toBytes("base_info"),Bytes.toBytes("name"));
            Cell cAge = r.getColumnLatestCell(Bytes.toBytes("base_info"),Bytes.toBytes("age"));
            int id = Bytes.toInt(CellUtil.cloneValue(cId));
            String name = Bytes.toString(CellUtil.cloneValue(cName));
            int age = Bytes.toInt(CellUtil.cloneValue(cAge));
            System.out.println("-----------------------------");
            System.out.println(rowId + "," + id + "," + age + "," + name);

        }
        scanner.close();
        table.close();
        conn.close();
    }








}
