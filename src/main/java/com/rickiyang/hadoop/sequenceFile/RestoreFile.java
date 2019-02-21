package com.rickiyang.hadoop.sequenceFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;

/**
 * @Author yangyue
 * @Date Created in 下午6:03 2019/2/21
 * @Modified by:
 * @Description: 将多个小文件合并成的sequenceFile文件还原为多个文件
 **/
public class RestoreFile {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Option option = Reader.file(new Path("/mergeFile"));
        Reader reader = new SequenceFile.Reader(conf, option);
        Text fileName = new Text();
        BytesWritable content = new BytesWritable();
        while (reader.next(fileName, content)) {
            System.out.println("文件名：" + fileName + ",文件内容：" + new String(content.getBytes()));
        }
    }
}