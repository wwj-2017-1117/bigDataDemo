package com.rickiyang.hadoop.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;


/**
 * @Author yangyue
 * @Date Created in 下午2:18 2019/2/22
 * @Modified by:
 * @Description: bloom的过滤模式，例子是通过筛选出相应的选择地址在候选列表中的交集的集合
 * bloom过滤必须要生成一个二进制数据，作为候选选择和处理的数据部分，定义候选的二进制数据结构：
 * 哈希函数个数k、二进制数组大小m、字符串数据n 之间存在着相关性，对于给定的m、n,当k=ln(2)*m/n时出错的概率是最小的。本文后面引用网上的数据证明。
 **/
public class BloomObject {


    //对数据进行转换的函数
    public static double k = 2.0; //调节二进制数组的相应的参数变量

    public static int getBloomSize(int numbersize, float falsePosError) {
        return  (int) (((-numbersize * (float) Math.log(falsePosError) / (float) Math.pow(Math.log(2), 2))) * k);
    }

    //获取hash函数的个数
    public static int getBloomHash(float numbersize, float longsize) {
        return (int) (numbersize / longsize * Math.log(2) * k);
    }

    /**
     * 创建bloom过滤器的数据文件，生成相应的二进制数据文件
     *
     * @param fileInput  输入路径
     * @param fileOutput 输出路径
     * @param errorRate  错误率
     * @param numberLong 输入字段长度
     * @throws Exception
     */
    public static void CreateBloomFile(String fileInput, String fileOutput, float errorRate, int numberLong) throws Exception {
        Configuration configuration = new Configuration();
        Path inputpath = new Path(fileInput);
        Path outputpath = new Path(fileOutput);
        FileSystem fileSystem = FileSystem.get(new URI(outputpath.toString()), configuration);
        if (fileSystem.exists(outputpath)) {
            fileSystem.deleteOnExit(outputpath);
        }
        //使用hadoop自带的bloom过滤函数
        int BloomSize = getBloomSize(numberLong, errorRate);
        int BloomHash = getBloomHash(BloomSize, numberLong);

        System.out.println("二进制数据的位数-----" + BloomSize);
        System.out.println("哈希函数的个数-----" + BloomHash);

        BloomFilter bloomFilter = new BloomFilter(BloomSize, BloomHash, Hash.MURMUR_HASH);//定义相应的bloom函数


        //把相应的结果存入到相应的输出文件中，正常情况下是hdfs的路径中，也可以是本地路径
        //利用hadoop的filesystem的系统的命令把相应的数据存储到hdfs的目录中
        FileSystem fileInputSystem = FileSystem.get(new URI(inputpath.toString()), configuration);
        String line;
        for (FileStatus fileStatus : fileInputSystem.listStatus(inputpath)) {
            //System.out.println(fileStatus.getPath());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputSystem.open(fileStatus.getPath())));
            while ((line = bufferedReader.readLine()) != null) {
                //存入到相应的bloom的API的变量中,bloom存放的是二进制的数据因此需要进行转换
                bloomFilter.add(new Key(line.getBytes("UTF-8")));
                System.out.println(line.getBytes("UTF-8"));
            }
            bufferedReader.close();
        }
        FSDataOutputStream fsDataOutputStream = fileInputSystem.create(new Path(outputpath + "/bloomtestlog"));
        bloomFilter.write(fsDataOutputStream);
        //fsDataOutputStream.flush(); //写入到缓存中的数据
        fsDataOutputStream.close();
    }

    public static void main(String[] args) throws Exception {
        BloomObject.CreateBloomFile("/Users/rickiyang/Documents/email.txt", "/Users/rickiyang/Documents/outputBloomFile", 0.01f, 2);

        DataInputStream dataInputStream = new DataInputStream(new FileInputStream("/Users/rickiyang/Documents/outputBloomFile/bloomtestlog"));
        BloomFilter filter = new BloomFilter();
        filter.readFields(dataInputStream);
        dataInputStream.close();
        //System.out.println();
        //hash函数过多导致即使是不同的数据多次的覆盖之后就会变成0
        String str = "lixiaoxiao@dd.com";
        if (filter.membershipTest(new Key(str.getBytes("UTF-8")))) {
            System.out.println(str.getBytes("UTF-8"));
            System.out.println("成功匹配二进制文件数据内容，Bloom二进制文件构造成功！");
        }
    }
}