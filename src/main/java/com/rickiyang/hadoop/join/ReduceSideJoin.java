package com.rickiyang.hadoop.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * @Author yangyue
 * @Date Created in 上午10:34 2019/2/22
 * @Modified by:
 * @Description: reduce端的join操作
 **/
public class ReduceSideJoin {

    public class CombineBean implements WritableComparable<CombineBean> {

        // 标志位，标明文件来源
        private Text flag;
        // 连接字段
        private Text joinKey;
        // 其余字段
        private Text others;

        public CombineBean() {
            this.flag = new Text();
            this.joinKey = new Text();
            this.others = new Text();
        }

        public Text getFlag() {
            return flag;
        }

        public void setFlag(Text flag) {
            this.flag = flag;
        }

        public Text getJoinKey() {
            return joinKey;
        }

        public void setJoinKey(Text joinKey) {
            this.joinKey = joinKey;
        }

        public Text getOthers() {
            return others;
        }

        public void setOthers(Text others) {
            this.others = others;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            this.flag.write(out);
            this.joinKey.write(out);
            this.others.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.flag.readFields(in);
            this.joinKey.readFields(in);
            this.others.readFields(in);
        }

        @Override
        public int compareTo(CombineBean o) {
            return this.joinKey.compareTo(o.getJoinKey());
        }
    }

    public class ReduceSideJoinMapper extends Mapper<Object, Text, Text, CombineBean> {

        private CombineBean combineBean = new CombineBean();
        private Text flag = new Text();
        private Text joinKey = new Text();
        private Text others = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            // 通过反射方式获取FileSplit
//            InputSplit split = context.getInputSplit();
//            Class<? extends InputSplit> splitClass = split.getClass();
//            FileSplit fileSplit = null;
//            if (splitClass.equals(FileSplit.class)) {
//                fileSplit = (FileSplit) split;
//            } else if (splitClass.getName().equals(
//                    "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
//                try {
//                    Method getInputSplitMethod = splitClass
//                            .getDeclaredMethod("getInputSplit");
//                    getInputSplitMethod.setAccessible(true);
//                    fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
//                } catch (Exception e) {
//                    throw new IOException(e);
//                }
//            }
//            String pathName = fileSplit.getPath().toString();

            FileSplit split = (FileSplit) context.getInputSplit();
            String pathName = split.getPath().toString();

            String[] data = value.toString().split("\\|");
            if (pathName.contains("student.csv")) {
                flag.set("0");
                joinKey.set(data[1]);
                others.set(data[0] + "#" + data[2]);
            } else if (pathName.contains("studentscore1.csv")) {
                flag.set("1");
                joinKey.set(data[0]);
                others.set(data[1] + "#" + data[2]);
            }
            combineBean.setFlag(flag);
            combineBean.setJoinKey(joinKey);
            combineBean.setOthers(others);
            context.write(combineBean.getJoinKey(), combineBean);
        }
    }

    public class ReduceSideJoinReducer extends Reducer<Text, CombineBean, Text, Text> {

        private List<Text> leftTable = new ArrayList<>();
        private List<Text> rightTable = new ArrayList<>();
        private Text resultVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<CombineBean> beans, Context context) throws IOException, InterruptedException {
            leftTable.clear();
            rightTable.clear();

            for (CombineBean bean : beans) {
                String flag = bean.getFlag().toString();
                if ("0".equals(flag.trim())) {
                    leftTable.add(bean.getOthers());
                }
                if ("1".equals(flag.trim())) {
                    rightTable.add(bean.getOthers());
                }
            }

            for (Text leftData : leftTable) {
                for (Text rightData : rightTable) {
                    resultVal.set(leftData + "#" + rightData);
                    context.write(key, resultVal);
                }
            }
        }

    }

    public class ReduceSideJoinJob extends Configured implements Tool {

        @Override
        public int run(String[] args) throws Exception {
            Configuration conf = getConf();

            Job job = Job.getInstance(conf, "ReduceSideJoin");

            job.setJarByClass(ReduceSideJoinJob.class);
            job.setMapperClass(ReduceSideJoinMapper.class);
            job.setReducerClass(ReduceSideJoinReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(CombineBean.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job, new Path(args[0]), SequenceFileInputFormat.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            return job.waitForCompletion(true) ? 0 : 1;
        }

    }

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        try {
            int returnCode = ToolRunner.run(new SemiJoin(), args);
            System.exit(returnCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}