package com.rickiyang.hadoop.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 */
public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    /**
     * reduce
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0 ;
        for(IntWritable iw : values){
            count = count + iw.get() ;
        }
        String tno = Thread.currentThread().getName();
        System.out.println(tno + " : WCReducer :" + key.toString() + "=" + count);
        context.write(key,new IntWritable(count));
    }
}
