package com.rickiyang.hadoop.secondCompare;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author yangyue
 * @Date Created in 下午5:44 2019/2/20
 * @Modified by:
 * @Description: 二次排序
 **/
public class SecondCompare {

    //定义一个类实现WritableComparable接口，分别定义两个字段，简单来说就是你想要依据排序的两个字段
    public static class TwoIntWritable implements WritableComparable<TwoIntWritable> {
        private Integer number1;
        private Integer number2;

        public Integer getNumber1() {
            return number1;
        }

        public void setNumber1(Integer number1) {
            this.number1 = number1;
        }

        public Integer getNumber2() {
            return number2;
        }

        public void setNumber2(Integer number2) {
            this.number2 = number2;
        }

        //实现序列化过程
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(number1);
            out.writeInt(number2);
        }

        //实现反序列化的过程，在反序列化的过程中，严格按照输出的顺序进行反序列化，
        //先输出的是number1，所以this.number1 = in.readInt()在前
        @Override
        public void readFields(DataInput in) throws IOException {
            this.number1 = in.readInt();
            this.number2 = in.readInt();
        }

        //实现比较规则
        @Override
        public int compareTo(TwoIntWritable o) {
            if (this.number1.equals(o.number1)) {
                //第一个相同的话就比较第二个字段
                return this.getNumber2().compareTo(o.getNumber2());
            } else {
                //第一个不相同的话就直接用第一个比较
                return this.getNumber1().compareTo(o.getNumber1());
            }
        }
    }

    //定义一个新的partitioner类继承并partitioner类，该分区规则只用第一个字段来分区(即相同的key值打上相同的标识进入相同的reduce进行处理)
    public static class TwoIntPartitioner extends Partitioner<TwoIntWritable, Object> {

        @Override
        public int getPartition(TwoIntWritable key, Object value, int numPatitions) {

            return (key.getNumber1().hashCode() & Integer.MAX_VALUE) % numPatitions;
        }
    }

    public static class SecondCompareMapper extends Mapper<LongWritable, Text, TwoIntWritable, NullWritable> {
        private String[] infos;
        private TwoIntWritable outputKey = new TwoIntWritable();
        private NullWritable outputValue = NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, TwoIntWritable, NullWritable>.Context context)
                throws IOException, InterruptedException {
            infos = value.toString().split("\\s");
            outputKey.setNumber1(Integer.valueOf(infos[0]));
            outputKey.setNumber2(Integer.valueOf(infos[1]));
            context.write(outputKey, outputValue);
        }
    }

    public static class SecondCompareReduce extends Reducer<TwoIntWritable, NullWritable, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        protected void reduce(TwoIntWritable key, Iterable<NullWritable> values,
                              Reducer<TwoIntWritable, NullWritable, Text, Text>.Context context)
                throws IOException, InterruptedException {
            //加for循环是为了进入reduce处理后避免去重效果
            for (NullWritable value : values) {
                outputKey.set(key.getNumber1() + "");
                outputValue.set(key.getNumber2() + "");
                context.write(outputKey, outputValue);
            }
            outputKey.set("---------------------");
            outputValue.set("---------------------");
            context.write(outputKey, outputValue);
        }
    }

    public static class GroupingComparator extends WritableComparator {
        //调用父类的构造方法来告诉mr引擎本比较器比较的是什么类型的数据
        public GroupingComparator() {
            super(TwoIntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TwoIntWritable ta = (TwoIntWritable) a;
            TwoIntWritable tb = (TwoIntWritable) b;
            return ta.getNumber1().compareTo(tb.getNumber1());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SecondCompare.class);
        job.setJobName("二次排序");

        job.setMapperClass(SecondCompareMapper.class);
        job.setReducerClass(SecondCompareReduce.class);
        job.setPartitionerClass(TwoIntPartitioner.class);

        //job.setOutputKeyClass和job.setOutputValueClass上述两个是设置的reducer的输出类型，
        //如果reducer的输出kv类型和map的输出kv类型一样则不用单独设置map的输出kv类型
        //否则需要单独设置map的kv输出类型
        job.setMapOutputKeyClass(TwoIntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);

        job.setGroupingComparatorClass(GroupingComparator.class);

        FileInputFormat.addInputPath(job, new Path("/bd32/sesort.txt"));
        Path outputDir = new Path("/bd32/sesortoutput");
        outputDir.getFileSystem(conf).delete(outputDir, true);
        FileOutputFormat.setOutputPath(job, outputDir);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}