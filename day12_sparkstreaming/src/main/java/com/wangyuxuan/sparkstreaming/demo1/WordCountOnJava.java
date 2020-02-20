package com.wangyuxuan.sparkstreaming.demo1;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author wangyuxuan
 * @date 2020/2/19 9:22
 * @description SparkStreaming单词统计java版本
 */
public class WordCountOnJava {
    public static void main(String[] args) throws InterruptedException {
        // 设置了日志的级别
        Logger.getLogger("org").setLevel(Level.ERROR);
        // 步骤一：初始化程序入口
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCountOnJava");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        // 步骤二：获取数据流
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        // 步骤三：数据处理
        JavaDStream<String> words = lines.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(",")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);
        // 步骤四： 数据输出
        wordCounts.print();
        // 步骤五：启动任务
        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }
}
