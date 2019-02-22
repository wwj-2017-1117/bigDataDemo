package com.rickiyang.MultiInputData;

import com.rickiyang.Utils.JobUtilJar;
import com.rickiyang.Utils.MRDPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

/**
 * @Author yangyue
 * @Date Created in 下午5:14 2019/2/22
 * @Modified by:
 * @Description:
 **/
public class MultiInputDataApp extends Configured implements Tool {

    public class PartitionedUserPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            final String MF = key.toString();//获取到性别
            if ("M".equals(MF)) {
                return 0 % numPartitions;
            } else if ("F".equals(MF)) {
                return 1 % numPartitions;
            } else {
                return 2 % numPartitions;
            }
        }
    }

    /**
     * 评论分类
     */
    public class CommentsMapper extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // 存储解析的内容Map
            Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
            String postId = parsed.get("PostId");//评论对应的帖子ID
            if (postId == null) {
                return;
            }
            //使用外健ID
            outkey.set(postId);
            // 标记该内容为评论，输出
            outvalue.set("C" + value.toString());
            context.write(outkey, outvalue);
        }
    }

    /**
     * 帖子类
     */
    public class PostMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
            String postId = parsed.get("Id");
            if (postId == null) {
                return;
            }
            outKey.set(postId);
            outValue.set("P" + value.toString());
            context.write(outKey, outValue);
        }
    }

    /**
     * 回答分类
     */
    public class QuestionAnswerMapper extends Mapper<Object, Text, Text, Text> {
        private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 解析 帖子/评论 XML 的元素
            Element post = DataOrganUtility.getXmlElementFromString(dbf, value.toString());

            int postType = Integer.parseInt(post.getAttribute("PostTypeId"));

            // 如果帖子类型 postType 是 1, 那该帖子属于问题
            if (postType == 1) {
                outkey.set(post.getAttribute("Id"));
                outvalue.set("Q" + value.toString());
            } else {
                //否则属于回答
                outkey.set(post.getAttribute("ParentId"));
                outvalue.set("A" + value.toString());
            }
            context.write(outkey, outvalue);
        }
    }


    /**
     * 评论帖子Reduce类：创建有层次的XML对象。判断map输出的value，有标示P的表明该value是帖子，
     * 去掉P，将其存储到字符串中，否则表明是评论，去掉C，存储到数组中。然后，序列化为XML对象输出。<p>
     */
    public class PostCommentHierarchyReducer extends Reducer<Text, Text, Text, NullWritable> {

        private ArrayList<String> comments = new ArrayList<>();//存储评论

        private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();//XML创建对象工厂

        private String post = null;//存储帖子内容


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws InterruptedException, IOException {
            post = null;
            comments.clear();
            // 遍历输出的value，获取内容
            for (Text t : values) {
                // 如果该value是帖子，存储起来，并去掉标示P
                if (t.charAt(0) == 'P') {
                    post = t.toString().substring(1).trim();
                } else {
                    //否则标示是评论，去掉前面的标示C，存储到数组中（一个帖子会有多个评论）
                    comments.add(t.toString().substring(1).trim());
                }
            }
            // 帖子内容不为空时输出
            if (post != null) {
                // 根据帖子和评论，创建层次结构XML 字符串
                String postWithCommentChildren = DataOrganUtility.nestElements(dbf, post, comments);
                // 输出
                context.write(new Text(postWithCommentChildren), NullWritable.get());
            }
        }
    }

    public class QuestionAnswerReducer extends Reducer<Text, Text, Text, NullWritable> {
        private ArrayList<String> answers = new ArrayList<>();
        private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        private String question = null;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
            question = null;
            answers.clear();
            // 遍历输出内容
            for (Text t : values) {
                // 如果帖子是问题 存储，去掉标示
                if (t.charAt(0) == 'Q') {
                    question = t.toString().substring(1).trim();
                } else {
                    answers.add(t.toString().substring(1).trim());
                }
            }
            if (question != null) {
                String postWithCommentChildren = DataOrganUtility.nestElements(dbf, question, answers);
                context.write(new Text(postWithCommentChildren), NullWritable.get());
            }
        }
    }


    /**
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        //环境变量设置
        File jarFile = JobUtilJar.createTempJar(JobUtilJar.outJarPath);
        ClassLoader classLoader = JobUtilJar.getClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);

        //获取连接hadoop集群配置，默认加载当前线程相关配置
        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf, "PostComments Hadoop Job");
        job.setJarByClass(MultiInputDataApp.class);
        ((JobConf) job.getConfiguration()).setJar(jarFile.toString());
        //设置多数据源的Mapper输入
        MultipleInputs.addInputPath(job, new Path(args[0]), org.apache.hadoop.mapreduce.InputFormat.class, PostMapper.class);//帖子输入
        //评论输入
        MultipleInputs.addInputPath(job, new Path(args[1]), org.apache.hadoop.mapreduce.InputFormat.class, CommentsMapper.class);
        /// 执行用户自定义reduce函数
        job.setReducerClass(PostCommentHierarchyReducer.class);

        //设置输出
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.OutputFormat.class);
        TextOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.out.println("PostComments Hadoop Job start!");

        //开始运行Job
        return (job.waitForCompletion(true) ? 0 : -1);
    }

    /**
     * 主程序
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String[] otherArgs = new String[2];
        // 计算原文件目录，需提前在里面存入文件
        otherArgs[0] = "hdfs://192.168.2.2:8020/post/";
        otherArgs[1] = "hdfs://192.168.2.2:8020/commnets/";
        // 计算后的计算结果存储目录，每次程序执行的结果目录不能相同，所以添加时间标签
        String time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        otherArgs[2] = "hdfs://192.168.2.2:8020/test_out/" + time;
        int exitCode = ToolRunner.run(new MultiInputDataApp(), otherArgs);
        if (exitCode == 0) {
            System.out.println("ok!");
        } else {
            System.out.println("error!");
        }
        System.exit(exitCode);
    }
}
