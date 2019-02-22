package com.rickiyang.hadoop.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author yangyue
 * @Date Created in 下午7:21 2019/2/21
 * @Modified by:
 * @Description: map端的关联关系
 **/
public class MapSideJoin {

    public static class MapSideJoinMap extends Mapper<LongWritable, Text, Text, Text> {
        private Map<Integer, String> littleTableContent;
        private String[] infos;
        private Text outputValue = new Text();

        //加载小表的数据，把关联字段作为key放入littleTableContent中
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException {
            littleTableContent = new HashMap<>();
            //获取分布式缓存文件的路径
            URI[] uris = context.getCacheFiles();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            for (URI uri : uris) {
                FSDataInputStream in = fs.open(new Path(uri.getPath()));
                InputStreamReader reader = new InputStreamReader(in, "UTF-8");
                BufferedReader br = new BufferedReader(reader);
                String line = br.readLine();
                while (line != null) {
                    infos = line.split(",");
                    littleTableContent.put(Integer.valueOf(infos[0]), infos[1] + "," + infos[2] + "," + infos[3]);
                    line = br.readLine();
                }
            }
        }

        //处理大表数据，用用户id作为key去littleTableContent找到相应的数据，把value关联到大表数据后输出即可
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            infos = value.toString().split(",");
            String userInfo = littleTableContent.get(Integer.valueOf(infos[0]));
            outputValue.set(userInfo);
            context.write(value, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MapSideJoin.class);
        job.setJobName("map端关联");
        job.setMapperClass(MapSideJoinMap.class);
        //如果不需要reducer，必须把reducer的数量置为0
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //把小表的路径设置为分布式缓存文件
        Path littleFilePath = new Path("/bd32/joinTest/student.csv");
        URI littleFileURI = littleFilePath.toUri();
        job.setCacheFiles(new URI[]{littleFileURI});
        //job.addCacheFile(new URI("file:/D:/srcdata/mapjoincache/pdts.txt"));
        //job.addCacheFile(new URI("hdfs://centos-aaron-h1:9000/rjoin/mapjoincache/product.txt"));

        //设置输入输出
        FileInputFormat.addInputPath(job, new Path("/bd32/joinTest/studentscore1.csv"));
        Path outputDir = new Path("/bd32/mapjoinoutput");
        outputDir.getFileSystem(conf).delete(outputDir, true);
        FileOutputFormat.setOutputPath(job, outputDir);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
