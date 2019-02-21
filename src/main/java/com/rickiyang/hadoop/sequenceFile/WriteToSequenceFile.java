package com.rickiyang.hadoop.sequenceFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;

/**
 * @Author yangyue
 * @Date Created in 下午5:57 2019/2/21
 * @Modified by:
 * @Description: sequenceFile 文件的写入
 *
 **/
public class WriteToSequenceFile {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Option option1 = SequenceFile.Writer.file(new Path("/sequencefile"));
        Option option2 = SequenceFile.Writer.keyClass(Text.class);
        Option option3 = SequenceFile.Writer.valueClass(IntWritable.class);
        Writer writer = SequenceFile.createWriter(conf, option1, option2, option3);

        for (int i = 0; i < 100; i++) {
            writer.append(new Text("record" + i), new IntWritable(i));
        }
        writer.hflush();
        writer.close();
    }
}