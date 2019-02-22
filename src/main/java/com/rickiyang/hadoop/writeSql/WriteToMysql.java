package com.rickiyang.hadoop.writeSql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class WriteToMysql {

    public static class WriteToMysqlMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private String[] infos;
        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            infos = value.toString().split("\\|");
            outputKey.set(infos[1]);
            outputValue.set(Integer.valueOf(infos[3]));
            context.write(outputKey, outputValue);
        }
    }

    //自定义类型封装要保存到数据库中的数据，同时要继承DBWritable
    public static class OrderCountWritable implements Writable, DBWritable {
        private Integer orderId;
        private Integer productNum;

        public Integer getOrderId() {
            return orderId;
        }

        public void setOrderId(Integer orderId) {
            this.orderId = orderId;
        }

        public Integer getProductNum() {
            return productNum;
        }

        public void setProductNum(Integer productNum) {
            this.productNum = productNum;
        }

        public void write(PreparedStatement state) throws SQLException {
            state.setInt(1, this.getOrderId());
            state.setInt(2, this.getProductNum());
        }

        public void readFields(ResultSet resultSet) throws SQLException {
            this.orderId = resultSet.getInt("order_id");
            this.productNum = resultSet.getInt("product_num");
        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(this.orderId);
            out.writeInt(this.productNum);
        }

        public void readFields(DataInput in) throws IOException {
            this.orderId = in.readInt();
            this.productNum = in.readInt();
        }
    }

    public static class WriteToMysqlReduce extends Reducer<Text, IntWritable, OrderCountWritable, NullWritable> {
        private int num;
        private OrderCountWritable outputKey = new OrderCountWritable();
        private NullWritable outputValue = NullWritable.get();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, OrderCountWritable, NullWritable>.Context context)
                throws IOException, InterruptedException {
            num = 0;
            for (IntWritable value : values) {
                num += value.get();
            }
            outputKey.setOrderId(Integer.valueOf(key.toString()));
            outputKey.setProductNum(num);
            context.write(outputKey, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.53.2:3306/emp", "root", "123456");
        Job job = Job.getInstance(conf);
        job.setJarByClass(WriteToMysql.class);
        job.setJobName("写MySQL");

        job.setMapperClass(WriteToMysqlMap.class);
        job.setReducerClass(WriteToMysqlReduce.class);

        job.addFileToClassPath(new Path("/tmp/mysql-connector-java-5.1.39.jar"));
        job.setOutputKeyClass(OrderCountWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(DBOutputFormat.class);
        DBOutputFormat.setOutput(job, "order_count", "order_id", "product_num");
        FileInputFormat.addInputPath(job, new Path("/rickiyang/orderdata/order_items"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
