package com.rickiyang.hadoop.secondSorted;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class MaxTempApp {


    /**
     * 自定义组合key
     */
    public class ComboKey implements WritableComparable<ComboKey> {
        private int year;
        private int temp;

        public int getYear() {
            return year;
        }

        public void setYear(int year) {
            this.year = year;
        }

        public int getTemp() {
            return temp;
        }

        public void setTemp(int temp) {
            this.temp = temp;
        }

        /**
         * 对key进行比较实现
         */
        @Override
        public int compareTo(ComboKey o) {
            int y0 = o.getYear();
            int t0 = o.getTemp();
            //年份相同(升序)
            if (year == y0) {
                //气温降序
                return -(temp - t0);
            } else {
                return year - y0;
            }
        }

        /**
         * 串行化过程
         */
        @Override
        public void write(DataOutput out) throws IOException {
            //年份
            out.writeInt(year);
            //气温
            out.writeInt(temp);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            year = in.readInt();
            temp = in.readInt();
        }
    }

    /**
     * ComboKeyComparator
     */
    public class ComboKeyComparator extends WritableComparator {

        protected ComboKeyComparator() {
            super(ComboKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ComboKey k1 = (ComboKey) a;
            ComboKey k2 = (ComboKey) b;
            return k1.compareTo(k2);
        }
    }

    /**
     * WCTextMapper
     */
    public class MaxTempMapper extends Mapper<LongWritable, Text, ComboKey, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arr = line.split(" ");

            ComboKey keyOut = new ComboKey();
            keyOut.setYear(Integer.parseInt(arr[0]));
            keyOut.setTemp(Integer.parseInt(arr[1]));
            context.write(keyOut, NullWritable.get());
        }
    }


    /**
     * Reducer
     */
    public class MaxTempReducer extends Reducer<ComboKey, NullWritable, IntWritable, IntWritable> {

        /**
         */
        @Override
        protected void reduce(ComboKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int year = key.getYear();
            int temp = key.getTemp();
            System.out.println("==============>reduce");
            for (NullWritable v : values) {
                System.out.println(key.getYear() + " : " + key.getTemp());
            }
            context.write(new IntWritable(year), new IntWritable(temp));
        }
    }

    /**
     * 按照年份进行分组对比器实现
     */
    public class YearGroupComparator extends WritableComparator {

        protected YearGroupComparator() {
            super(ComboKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ComboKey k1 = (ComboKey) a;
            ComboKey k2 = (ComboKey) b;
            return k1.getYear() - k2.getYear();
        }
    }

    /**
     * 自定义分区类
     */
    public class YearPartitioner extends Partitioner<ComboKey, NullWritable> {

        @Override
        public int getPartition(ComboKey key, NullWritable nullWritable, int numPartitions) {
            int year = key.getYear();
            return year % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");

        Job job = Job.getInstance(conf);

        //设置job的各种属性
        job.setJobName("SecondarySortApp");                        //作业名称
        job.setJarByClass(MaxTempApp.class);                 //搜索类
        job.setInputFormatClass(TextInputFormat.class); //设置输入格式

        //添加输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTempMapper.class);             //mapper类
        job.setReducerClass(MaxTempReducer.class);           //reducer类

        //设置Map输出类型
        job.setMapOutputKeyClass(ComboKey.class);            //
        job.setMapOutputValueClass(NullWritable.class);      //

        //设置ReduceOutput类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);         //

        //设置分区类
        job.setPartitionerClass(YearPartitioner.class);
        //设置分组对比器
        job.setGroupingComparatorClass(YearGroupComparator.class);
        //设置排序对比器
        job.setSortComparatorClass(ComboKeyComparator.class);

        job.setNumReduceTasks(3);                           //reduce个数
        //
        job.waitForCompletion(true);
    }
}