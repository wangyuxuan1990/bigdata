package com.wangyuxuan.spark.demo1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author wangyuxuan
 * @date 2020/1/13 11:46
 * @description 利用java语言开发spark的单词统计程序
 */
public class JavaWordCount {
    public static void main(String[] args) {
        //1、创建SparkConf对象
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]");
        //2、构建JavaSparkContext对象
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //3、读取数据文件
        JavaRDD<String> data = jsc.textFile("words.txt");
        //4、切分每一行获取所有的单词   scala:  data.flatMap(x=>x.split(" "))
        JavaRDD<String> wordsJavaRDD = data.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                String[] words = line.split(" ");
                return Arrays.asList(words).iterator();
            }
        });
        //5、每个单词计为1    scala:  wordsJavaRDD.map(x=>(x,1))
        JavaPairRDD<String, Integer> wordAndOne = wordsJavaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        //6、相同单词出现的1累加    scala:  wordAndOne.reduceByKey((x,y)=>x+y)
        JavaPairRDD<String, Integer> result = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //按照单词出现的次数降序 (单词，次数)  -->(次数,单词).sortByKey----> (单词，次数)
        JavaPairRDD<Integer, String> reverseJavaRDD = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer, String>(t._2, t._1);
            }
        });

        JavaPairRDD<String, Integer> sortedRDD = reverseJavaRDD.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<String, Integer>(t._2, t._1);
            }
        });

        //7、收集打印
        List<Tuple2<String, Integer>> finalResult = sortedRDD.collect();

        for (Tuple2<String, Integer> t : finalResult) {
            System.out.println("单词: " + t._1 + "\t次数: " + t._2);
        }

        jsc.stop();
    }
}
