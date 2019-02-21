package com.rickiyang.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author yangyue
 * @Date Created in 下午7:09 2019/2/21
 * @Modified by:
 * @Description: 多个mapreduce串联
 **/
public class ChainTest {

    //过滤掉销量大于10000的数据
    public static class ChainTestMap1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private String[] infos;
        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            infos = value.toString().split("\\s");
            if (Integer.valueOf(infos[1]) <= 10000) {
                outputKey.set(infos[0]);
                outputValue.set(Integer.valueOf(infos[1]));
                context.write(outputKey, outputValue);
            }
        }
    }

    //过滤掉销量100-10000的数据
    public static class ChainTestMap2 extends Mapper<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            if (value.get() <= 100 || value.get() >= 10000) {
                context.write(key, value);
            }
        }
    }

    //计算每类商品的总销售量
    public static class ChainTestReduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private int sum;
        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }

    public static class ChainTestMap3 extends Mapper<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            if (key.toString().length() <= 3) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ChainTest.class);
        job.setJobName("mr串联");

        ChainMapper.addMapper(job, ChainTestMap1.class, LongWritable.class, Text.class, Text.class, IntWritable.class, conf);
        ChainMapper.addMapper(job, ChainTestMap2.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);
        ChainReducer.setReducer(job, ChainTestReduce1.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);
        ChainReducer.addMapper(job, ChainTestMap3.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);

        //设置输入输出文件
        FileInputFormat.addInputPath(job, new Path("/bd32/productsales.txt"));
        Path outputDir = new Path("/bd32/chaintestoutput");
        outputDir.getFileSystem(conf).delete(outputDir, true);
        FileOutputFormat.setOutputPath(job, outputDir);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
