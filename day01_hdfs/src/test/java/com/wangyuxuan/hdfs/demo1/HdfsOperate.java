package com.wangyuxuan.hdfs.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/11/28 15:42
 * @description
 */
public class HdfsOperate {

    @Test
    public void mkdirToHdfs() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.mkdirs(new Path("/kaikeba/dir1"));
        fileSystem.close();
    }

}
