package com.rickiyang.hadoop.UserLoginCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * @Author yangyue
 * @Date Created in 下午1:35 2019/2/20
 * @Modified by:
 * @Description: 抽样分析
 **/


public class UserLoginOrder {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //1、定义输入抽样
        RandomSampler<IntWritable, Text> sampler = new RandomSampler<IntWritable, Text>(0.8, 5);
        //2.设置分区文件位置
        Path partitonerFile = new Path("/rickiyang/cyxorder");
        TotalOrderPartitioner.setPartitionFile(conf, partitonerFile);

        Job job = Job.getInstance(conf);
        job.setJarByClass(UserLoginOrder.class);
        job.setJobName("对用户登录次数进行全排序");

        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setNumReduceTasks(1);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/rickiyang/cyxgrouptop1/part-r-00000"));
        Path outputDir = new Path("/rickiyang/cyxgrouptop2");
        outputDir.getFileSystem(conf).delete(outputDir, true);
        FileOutputFormat.setOutputPath(job, outputDir);
        InputSampler.writePartitionFile(job, sampler);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}