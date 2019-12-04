package com.wangyuxuan.hdfs.demo3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author wangyuxuan
 * @date 2019/12/4 14:31
 * @description Sequence Files Write
 */
public class SequenceFileWriteNewVersion {
    // 模拟数据源
    private static final String[] DATA = {
            "The Apache Hadoop software library is a framework that allows for the distributed processing of large data sets across clusters of computers using simple programming models.",
            "It is designed to scale up from single servers to thousands of machines, each offering local computation and storage.",
            "Rather than rely on hardware to deliver high-availability, the library itself is designed to detect and handle failures at the application layer",
            "o delivering a highly-available service on top of a cluster of computers, each of which may be prone to failures.",
            "Hadoop Common: The common utilities that support the other Hadoop modules."
    };

    public static void main(String[] args) throws URISyntaxException, IOException {
        // 输出路径：要生成的SequenceFile文件名
        String uri = "hdfs://node01:8020/writeSequenceFile";
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(uri), configuration);
        // 向HDFS上的此SequenceFile文件写数据
        Path path = new Path(uri);
        // 因为SequenceFile每个record是键值对的
        // 指定key类型
        IntWritable key = new IntWritable();
        // 指定value类型
        Text value = new Text();

        // 创建向SequenceFile文件写入数据时的一些选项
        // 要写入的SequenceFile的路径
        SequenceFile.Writer.Option pathOption = SequenceFile.Writer.file(path);
        // record的key类型选项
        SequenceFile.Writer.Option keyOption = SequenceFile.Writer.keyClass(IntWritable.class);
        // record的value类型选项
        SequenceFile.Writer.Option valueOption = SequenceFile.Writer.valueClass(Text.class);
        // SequenceFile压缩方式：NONE | RECORD | BLOCK三选一
        SequenceFile.Writer.Option compressOption = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD);
        SequenceFile.Writer writer = SequenceFile.createWriter(configuration, pathOption, keyOption, valueOption, compressOption);

        for (int i = 0; i < 100000; i++) {
            // 分别设置key、value值
            key.set(100 - i);
            value.set(DATA[i % DATA.length]);
            System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
            // 在SequenceFile末尾追加内容
            writer.append(key, value);
        }

        // 关闭流
        IOUtils.closeStream(writer);
    }
}
