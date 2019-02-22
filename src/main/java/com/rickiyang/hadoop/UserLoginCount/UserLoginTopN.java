package com.rickiyang.hadoop.UserLoginCount;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * @Author yangyue
 * @Date Created in 下午1:34 2019/2/20
 * @Modified by:
 * @Description: 输出文件使用SequenceFile的形式
 **/

public class UserLoginTopN {
    public static class UserLoginTopNMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private String[] infos;
        private Text outputKey = new Text("");
        private IntWritable outputValue = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            infos = value.toString().split("\\s");
            outputKey.set(infos[0]);
            context.write(outputKey, outputValue);
        }
    }

    public static class UserLoginTopNReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
        private int sum;
        private IntWritable outputKey = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            outputKey.set(sum);
            context.write(outputKey, key);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UserLoginTopN.class);
        job.setJobName("行为个数聚合");

        job.setMapperClass(UserLoginTopNMap.class);
        job.setReducerClass(UserLoginTopNReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);


        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/rickiyang/user-logs-large.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/rickiyang/cyxgrouptop1"));

        System.exit(job.waitForCompletion(true)?0:1);

    }
}