package com.rickiyang.hadoop.allSorted;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * WCTextMapper
 */
public class MaxTempMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable>{

    @Override
    protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
