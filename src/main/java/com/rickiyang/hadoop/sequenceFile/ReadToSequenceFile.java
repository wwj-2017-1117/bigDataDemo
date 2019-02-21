package com.rickiyang.hadoop.sequenceFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;

/**
 * @Author yangyue
 * @Date Created in 下午5:56 2019/2/21
 * @Modified by:
 * @Description: sequenceFile 文件的读取
 **/


public class ReadToSequenceFile {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Option option = Reader.file(new Path("/sequencefile"));
        Reader reader = new Reader(conf, option);
        Text key = new Text();
        IntWritable value = new IntWritable();
        while (reader.next(key, value)) {
            System.out.println("key:" + key);
            System.out.println("value:" + value);
        }
    }

}