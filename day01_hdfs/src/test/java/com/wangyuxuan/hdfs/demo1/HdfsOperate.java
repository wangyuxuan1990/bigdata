package com.wangyuxuan.hdfs.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author wangyuxuan
 * @date 2019/11/28 15:42
 * @description 开发hdfs的javaAPI操作
 */
public class HdfsOperate {

    /**
     * 创建文件夹
     *
     * @throws IOException
     */
    @Test
    public void mkdirToHdfs() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.mkdirs(new Path("/kaikeba/dir1"));
        fileSystem.close();
    }

    /**
     * 文件上传
     *
     * @throws IOException
     */
    @Test
    public void uploadFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyFromLocalFile(new Path("file:///d:\\hello.txt"), new Path("hdfs://node01:8020/kaikeba/dir1"));
        fileSystem.close();
    }

    /**
     * 文件下载
     *
     * @throws IOException
     */
    @Test
    public void downloadFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyToLocalFile(new Path("hdfs://node01:8020/kaikeba/dir1/hello.txt"), new Path("file:///d:\\hello2.txt"));
        fileSystem.close();
    }

    /**
     * 文件重命名
     *
     * @throws IOException
     */
    @Test
    public void renameFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.rename(new Path("hdfs://node01:8020/kaikeba/dir1/hello.txt"), new Path("hdfs://node01:8020/kaikeba/dir1/hello1.txt"));
        fileSystem.close();
    }

    /**
     * 文件删除
     *
     * @throws IOException
     */
    @Test
    public void deleteFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(new Path("hdfs://node01:8020/kaikeba/dir1/hello1.txt"), true);
        fileSystem.close();
    }

    /**
     * 查看文件相关信息
     *
     * @throws URISyntaxException
     * @throws IOException
     */
    @Test
    public void testListFiles() throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), configuration);
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getGroup());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }

        fileSystem.close();
    }

}
