package com.rickiyang.hadoop.UserLoginCount;

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

import java.io.IOException;

/**
 * @Author yangyue
 * @Date Created in 下午1:33 2019/2/20
 * @Modified by:
 * @Description: 统计用户访问次数
 **/


public class UserLoginCount {
    public static class UserLoginCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private String[] infos;
        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            infos = value.toString().split("\\s");
            outputKey.set(infos[0]);
            context.write(outputKey, outputValue);
        }
    }

    public static class UserLoginCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private int sum = 0;
        private IntWritable outputKey = new IntWritable(0);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            outputKey.set(sum);
            context.write(key, outputKey);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(UserLoginCount.class);
        job.setJobName("登录计数");

        job.setMapperClass(UserLoginCountMap.class);
        job.setCombinerClass(UserLoginCountReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("file:///Users/rickiyang/workspace/bigDataDemo/file/user-logs-large.txt"));
        Path outputPath = new Path("/Users/rickiyang/workspace/bigDataDemo/output");
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
