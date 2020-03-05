//package com.wangyuxuan.kafka.demo4;
//
//import kafka.serializer.StringDecoder;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.function.*;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import scala.Tuple2;
//
//import java.util.*;
//
///**
// * @author wangyuxuan
// * @date 2020/3/4 15:53
// * @description 与Kafka整合数据不丢失方案 0.8版本 Java
// */
//public class JavaWordCount {
//    public static void main(String[] args) {
//        Logger.getLogger("org").setLevel(Level.ERROR);
//        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[4]");
//        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));
//        String topics = "sparkstreaming";
//        String groupId = "test5";
//        String brokers = "node01:9092,node02:9092,node03:9092";
//        Set<String> topicSet = new HashSet<>(Arrays.asList(topics.split(",")));
//        Map<String, String> kafkaParams = new HashMap<>();
//        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
//        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//
//        ssc.addStreamingListener(new KafkaListener(TypeHelper.toScalaImmutableMap(kafkaParams)));
//
//        final KafkaManager kafkaManager = new KafkaManager(TypeHelper.toScalaImmutableMap(kafkaParams));
//
//        JavaPairInputDStream<String, String> myDStream = kafkaManager.createDirectStream(
//                ssc,
//                String.class,
//                String.class,
//                StringDecoder.class,
//                StringDecoder.class,
//                kafkaParams,
//                topicSet
//        );
//
//        myDStream.map(new Function<Tuple2<String, String>, String>() {
//            @Override
//            public String call(Tuple2<String, String> tuple) throws Exception {
//                return tuple._2;
//            }
//        }).flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String line) throws Exception {
//                return Arrays.asList(line.split(",")).iterator();
//            }
//        }).mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String word) throws Exception {
//                return new Tuple2<>(word, 1);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer a, Integer b) throws Exception {
//                return a + b;
//            }
//        }).foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
//            @Override
//            public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
//                rdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//                    @Override
//                    public void call(Tuple2<String, Integer> wordCount) throws Exception {
//                        System.out.println("单词：" + wordCount._1 + " " + "次数：" + wordCount._2);
//                    }
//                });
//            }
//        });
//
//        ssc.start();
//        try {
//            ssc.awaitTermination();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        ssc.stop();
//
//    }
//}
