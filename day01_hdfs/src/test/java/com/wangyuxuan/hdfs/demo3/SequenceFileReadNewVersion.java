package com.wangyuxuan.hdfs.demo3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/4 14:32
 * @description Sequence Files Read
 */
public class SequenceFileReadNewVersion {

    public static void main(String[] args) throws IOException {
        // 要读的SequenceFile
        String uri = "hdfs://node01:8020/writeSequenceFile";
        Configuration configuration = new Configuration();
        Path path = new Path(uri);

        // Read对象
        SequenceFile.Reader reader = null;

        try {
            // 读取SequenceFile的Reader的路径选项
            SequenceFile.Reader.Option pathOption = SequenceFile.Reader.file(path);
            // 实例化Reader对象
            reader = new SequenceFile.Reader(configuration, pathOption);
            // 根据反射，求出key类型
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
            //根据反射，求出value类型
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);

            long position = reader.getPosition();
            System.out.println(position);

            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                position = reader.getPosition();
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}
