package com.rickiyang.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author yangyue
 * @Date Created in 下午1:02 2018/12/10
 * @Modified by:
 * @Description:
 **/
public class JavaSparkStreamingWordCountApp {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("wc");
        conf.setMaster("spark://s201:7077");
        //创建Spark流应用上下文
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Seconds.apply(1));
        //创建socket离散流
        JavaReceiverInputDStream sock = jsc.socketTextStream("s201", 9999);
        //压扁
        JavaDStream<String> wordsDS = sock.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator call(String str) throws Exception {
                List<String> list = new ArrayList<>();
                String[] arr = str.split(" ");
                for (String s : arr) {
                    list.add(s);
                }
                return list.iterator();
            }
        });

        //映射成元组
        JavaPairDStream<String, Integer> pairDS = wordsDS.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 5);
            }
        });

        //聚合
        JavaPairDStream<String, Integer> countDS = pairDS.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //打印
        countDS.print();

        jsc.start();

        jsc.awaitTermination();
    }



}


