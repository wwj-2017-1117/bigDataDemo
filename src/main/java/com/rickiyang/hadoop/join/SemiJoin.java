package com.rickiyang.hadoop.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * @Author yangyue
 * @Date Created in 上午11:58 2019/2/22
 * @Modified by:
 * @Description: 半连接， 将数据放入缓存之前先过滤掉不需要的数据
 **/
public class SemiJoin extends Configured implements Tool {

    public class CombineValues implements WritableComparable<CombineValues> {
        private Text joinKey;//链接关键字
        private Text flag;//文件来源标志
        private Text secondPart;//除了链接键外的其他部分

        public void setJoinKey(Text joinKey) {
            this.joinKey = joinKey;
        }

        public void setFlag(Text flag) {
            this.flag = flag;
        }

        public void setSecondPart(Text secondPart) {
            this.secondPart = secondPart;
        }

        public Text getFlag() {
            return flag;
        }

        public Text getSecondPart() {
            return secondPart;
        }

        public Text getJoinKey() {
            return joinKey;
        }

        public CombineValues() {
            this.joinKey = new Text();
            this.flag = new Text();
            this.secondPart = new Text();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            this.joinKey.write(out);
            this.flag.write(out);
            this.secondPart.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.joinKey.readFields(in);
            this.flag.readFields(in);
            this.secondPart.readFields(in);
        }

        @Override
        public int compareTo(CombineValues o) {
            return this.joinKey.compareTo(o.getJoinKey());
        }

        @Override
        public String toString() {
            return "[flag=" + this.flag.toString() + ",joinKey=" + this.joinKey.toString() + ",secondPart=" + this.secondPart.toString() + "]";
        }
    }

    public class SemiJoinMapper extends Mapper<Object, Text, Text, CombineValues> {
        private CombineValues combineValues = new CombineValues();
        private HashSet<String> joinKeySet = new HashSet<>();
        private Text flag = new Text();
        private Text joinKey = new Text();
        private Text secondPart = new Text();

        /**
         * 将参加join的key从DistributedCache取出放到内存中，以便在map端将要参加join的key过滤出来。b
         */
        @Override
        protected void setup(Context context) throws IOException {
            FileSplit split = (FileSplit) context.getInputSplit();
            String pathName = split.getPath().toString();
            //读缓存文件，并放到mem中
            BufferedReader br = new BufferedReader(new FileReader(pathName));
            String line = br.readLine();
            while (null != line) {
                joinKeySet.add(line);
                line = br.readLine();
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //获得文件输入路径
            String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
            //数据来自tb_dim_city.dat文件,标志即为"0"
            if (pathName.endsWith("tb_dim_city.dat")) {
                String[] valueItems = value.toString().split("\\|");
                //过滤格式错误的记录
                if (valueItems.length != 5) {
                    return;
                }
                //过滤掉不需要参加join的记录
                if (joinKeySet.contains(valueItems[0])) {
                    flag.set("0");
                    joinKey.set(valueItems[0]);
                    secondPart.set(valueItems[1] + "\t" + valueItems[2] + "\t" + valueItems[3] + "\t" + valueItems[4]);
                    combineValues.setFlag(flag);
                    combineValues.setJoinKey(joinKey);
                    combineValues.setSecondPart(secondPart);
                    context.write(combineValues.getJoinKey(), combineValues);
                } else {
                    return;
                }
            }//数据来自于tb_user_profiles.dat，标志即为"1"
            else if (pathName.endsWith("tb_user_profiles.dat")) {
                String[] valueItems = value.toString().split("\\|");
                //过滤格式错误的记录
                if (valueItems.length != 4) {
                    return;
                }
                //过滤掉不需要参加join的记录
                if (joinKeySet.contains(valueItems[3])) {
                    flag.set("1");
                    joinKey.set(valueItems[3]);
                    secondPart.set(valueItems[0] + "\t" + valueItems[1] + "\t" + valueItems[2]);
                    combineValues.setFlag(flag);
                    combineValues.setJoinKey(joinKey);
                    combineValues.setSecondPart(secondPart);
                    context.write(combineValues.getJoinKey(), combineValues);
                } else {
                    return;
                }
            }
        }
    }

    public static class SemiJoinReducer extends Reducer<Text, CombineValues, Text, Text> {
        //存储一个分组中的左表信息
        private ArrayList<Text> leftTable = new ArrayList<>();
        //存储一个分组中的右表信息
        private ArrayList<Text> rightTable = new ArrayList<>();
        private Text secondPar = null;
        private Text output = new Text();

        /**
         * 一个分组调用一次reduce函数
         */
        @Override
        protected void reduce(Text key, Iterable<CombineValues> value, Context context)
                throws IOException, InterruptedException {
            leftTable.clear();
            rightTable.clear();
            /**
             * 将分组中的元素按照文件分别进行存放
             * 这种方法要注意的问题：
             * 如果一个分组内的元素太多的话，可能会导致在reduce阶段出现OOM，
             * 在处理分布式问题之前最好先了解数据的分布情况，根据不同的分布采取最
             * 适当的处理方法，这样可以有效的防止导致OOM和数据过度倾斜问题。
             */
            for (CombineValues cv : value) {
                secondPar = new Text(cv.getSecondPart().toString());
                //左表tb_dim_city
                if ("0".equals(cv.getFlag().toString().trim())) {
                    leftTable.add(secondPar);
                }
                //右表tb_user_profiles
                else if ("1".equals(cv.getFlag().toString().trim())) {
                    rightTable.add(secondPar);
                }
            }
            for (Text leftPart : leftTable) {
                for (Text rightPart : rightTable) {
                    output.set(leftPart + "\t" + rightPart);
                    context.write(key, output);
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "ReduceSideJoin");
        job.setJarByClass(SemiJoin.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SemiJoinMapper.class);

        //把小表的路径设置为分布式缓存文件
        Path littleFilePath = new Path("/rickiyang/joinTest/tb_dim_city.csv");
        URI littleFileURI = littleFilePath.toUri();
        job.setCacheFiles(new URI[]{littleFileURI});

        job.setReducerClass(SemiJoinReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //设置map的输出key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CombineValues.class);

        //设置reduce的输出key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            int returnCode = ToolRunner.run(new SemiJoin(), args);
            System.exit(returnCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
