package com.rickiyang.hadoop.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

/**
 * @Author yangyue
 * @Date Created in 下午2:14 2019/2/22
 * @Modified by:
 * @Description: mapper阶段使用布隆过滤器排除不必要的用户
 **/
public class BloomFilterReduce {


    public static class BloomFilteringMapper extends Mapper<Object, Text, Text, NullWritable> {
        private BloomFilter filter = new BloomFilter(1000,5,Hash.MURMUR_HASH);

        @Override
        protected void setup(Context context) throws IOException {
            //获取分布式缓存文件的路径
            URI[] uris = context.getCacheFiles();
            for (URI uri : uris) {
                DataInputStream strm = new DataInputStream(new FileInputStream(uri.getPath()));
                filter.readFields(strm);
                strm.close();
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String comment = value.toString();
            if (comment == null || comment.isEmpty()) {
                return;
            }
            StringTokenizer tokenizer = new StringTokenizer(comment);
            while (tokenizer.hasMoreTokens()) {
                String cleanWord = tokenizer.nextToken().replaceAll("'", "")
                        .replaceAll("[^a-zA-Z]", " ");
                if (cleanWord.length() > 0 && filter.membershipTest(new Key(cleanWord.getBytes()))) {
                    context.write(new Text(cleanWord), NullWritable.get());
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MapSideJoin.class);
        job.setJobName("map端关联");
        job.setMapperClass(BloomFilteringMapper.class);
        //如果不需要reducer，必须把reducer的数量置为0
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //把小表的路径设置为分布式缓存文件
        Path littleFilePath = new Path("/bd32/joinTest/aa.txt");
        URI littleFileURI = littleFilePath.toUri();
        job.setCacheFiles(new URI[]{littleFileURI});

        //设置输入输出
        FileInputFormat.addInputPath(job, new Path("/bd32/joinTest/bb.txt"));
        Path outputDir = new Path("/bd32/mapjoinoutput");
        outputDir.getFileSystem(conf).delete(outputDir, true);
        FileOutputFormat.setOutputPath(job, outputDir);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
