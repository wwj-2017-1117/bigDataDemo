package com.rickiyang.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author yangyue
 * @Date Created in 下午6:03 2019/2/20
 * @Modified by:
 * @Description: 实现倒排索引
 **/

public class ReverseIndex {

    //输出，key：word,value:filename
    public static class ReverseIndexMap extends Mapper<LongWritable, Text, Text, Text> {
        private String[] infos;
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            FileSplit fileInputSplit = (FileSplit) context.getInputSplit();
            String filePath = fileInputSplit.getPath().toString();
            outputValue.set(filePath);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            infos = value.toString().split("[\\s\\.,:-]");
            for (String word : infos) {
                outputKey.set(word);
                context.write(outputKey, outputValue);
            }
        }
    }

    //输出，key：word，value：filename(词频数)
    private static class ReverseIndexCombiner extends Reducer<Text, Text, Text, Text> {
        private Text outputValue = new Text();
        private int sum;
        private String filename;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            sum = 0;
            for (Text value : values) {
                if (sum == 0) {
                    filename = value.toString();
                }
                sum += 1;
            }
            outputValue.set(filename + "(" + sum + ")");
            context.write(key, outputValue);
        }
    }

    //输出：key：word，value：filename1(n1)---filename2(n2)---...
    private static class ReverseIndexReduce extends Reducer<Text, Text, Text, Text> {
        private Text outputValue = new Text();
        private StringBuffer filecount = new StringBuffer();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            filecount.delete(0, filecount.length());
            for (Text value : values) {
                if (filecount.length() == 0) {
                    filecount.append(value.toString());
                } else {
                    filecount.append("---");
                    filecount.append(value.toString());
                }
            }
            outputValue.set(filecount.toString());
            context.write(key, outputValue);
        }
    }

    //保证一个文件不会根据多个block划分成多个split
    public static class ReverseIndexInputFormat extends TextInputFormat {
        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ReverseIndex.class);
        job.setJobName("倒排索引");

        job.setMapperClass(ReverseIndexMap.class);
        job.setReducerClass(ReverseIndexReduce.class);
        job.setCombinerClass(ReverseIndexCombiner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/bd32/reindexfiles"));
        Path outputDir = new Path("/bd32/reindexoutput");
        outputDir.getFileSystem(conf).delete(outputDir, true);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setInputFormatClass(ReverseIndexInputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
