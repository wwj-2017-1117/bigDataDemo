package com.rickiyang.hadoop.sequenceFile;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.util.Collection;

/**
 * @Author yangyue
 * @Date Created in 下午6:02 2019/2/21
 * @Modified by:
 * @Description: 将小文件合并成一个大文件上传到hdfs上
 **/
public class MergeSmallFile {

    public static void main(String[] args) throws Exception {
        String sourcePath = "F:\\formerge";
        Configuration conf = new Configuration();
        Collection<File> files = FileUtils.listFiles(new File(sourcePath), null, true);
        Option option1 = Writer.file(new Path("/mergeFile"));
        Option option2 = Writer.keyClass(Text.class);
        Option option3 = Writer.valueClass(BytesWritable.class);

        Writer w = SequenceFile.createWriter(conf, option1, option2, option3);
        for (File file : files) {
            String key = file.getAbsolutePath();
            byte[] fileContent = FileUtils.readFileToByteArray(file);
            w.append(new Text(key), new BytesWritable(fileContent));
        }
    }

}