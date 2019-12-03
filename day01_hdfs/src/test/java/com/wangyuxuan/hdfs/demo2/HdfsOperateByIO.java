package com.wangyuxuan.hdfs.demo2;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author wangyuxuan
 * @date 2019/12/3 17:13
 * @description 通过IO流操作hdfs
 */
public class HdfsOperateByIO {

    /**
     * 通过io流进行数据上传操作
     *
     * @throws URISyntaxException
     * @throws IOException
     */
    @Test
    public void putFileToHDFS() throws URISyntaxException, IOException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), configuration);

        // 创建输入流
        FileInputStream fis = new FileInputStream(new File("d:\\hello.txt"));

        // 获取输出流
        FSDataOutputStream fos = fileSystem.create(new Path("hdfs://node01:8020/outresult.txt"));

        // 流拷贝
        IOUtils.copy(fis, fos);

        // 关闭流
        IOUtils.closeQuietly(fos);
        IOUtils.closeQuietly(fis);
        fileSystem.close();
    }

    /**
     * 通过io流进行数据下载操作
     *
     * @throws IOException
     * @throws URISyntaxException
     */
    @Test
    public void getFileFromHDFS() throws IOException, URISyntaxException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), configuration);

        // 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("d:\\hello3.txt"));

        // 获取输入流
        FSDataInputStream fis = fileSystem.open(new Path("hdfs://node01:8020/outresult.txt"));

        // 流拷贝
        IOUtils.copy(fis, fos);

        // 关闭流
        IOUtils.closeQuietly(fis);
        IOUtils.closeQuietly(fos);
        fileSystem.close();
    }

    /**
     * hdfs的小文件合并
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void mergeFile() throws URISyntaxException, IOException, InterruptedException {
        // 获取分布式文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "wangyuxuan");
        // 获取输出流
        FSDataOutputStream fos = fileSystem.create(new Path("hdfs://node01:8020/bigfile.xml"));
        // 获取本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        // 获取本地文件
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("file:///D:\\小文件合并"));
        for (FileStatus fileStatus : fileStatuses) {
            // 获取每一个本地文件路径
            Path path = fileStatus.getPath();
            // 读取本地小文件， 写入到hdfs的大文件里面去
            FSDataInputStream fis = localFileSystem.open(path);
            IOUtils.copy(fis, fos);
            IOUtils.closeQuietly(fis);
        }
        IOUtils.closeQuietly(fos);
        localFileSystem.close();
        fileSystem.close();
    }
}
