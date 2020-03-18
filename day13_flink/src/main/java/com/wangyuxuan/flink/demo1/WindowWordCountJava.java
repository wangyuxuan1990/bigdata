package com.wangyuxuan.flink.demo1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author wangyuxuan
 * @date 2020/3/18 9:44
 * @description WordCount Java版本
 */
public class WindowWordCountJava {
    public static void main(String[] args) throws Exception {
        // 步骤一：获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 步骤二：获取数据源
        DataStreamSource<String> dataStream = env.socketTextStream("node01", 9999);
        // 步骤三：执行逻辑操作
        DataStream<WordCount> wordAndOneStream = dataStream.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String line, Collector<WordCount> out) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    out.collect(new WordCount(word, 1L));
                }
            }
        });
        DataStream<WordCount> resultStream = wordAndOneStream
                .keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1)) // 每隔1秒计算最近2秒
                .sum("count");
        // 步骤四：结果打印
        resultStream.print();
        // 步骤五：任务启动
        env.execute("WindowWordCountJava");
    }

    public static class WordCount {
        public String word;
        public long count;

        // 记得要有这个空构建
        public WordCount() {
        }

        public WordCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
