package com.wangyuxuan.mr.demo6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/6 9:18
 * @description 自定义RecordReader
 */
public class MyInputFormatReader extends RecordReader<NullWritable, BytesWritable> {

    private FileSplit fileSplit;

    private Configuration configuration;

    private BytesWritable bytesWritable;

    /**
     * 读取文件标识
     */
    private boolean flag = false;

    /**
     * 初始化的方法  只在初始化的时候调用一次.只要拿到了文件的切片，就拿到了文件的内容
     *
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
        this.configuration = context.getConfiguration();
        bytesWritable = new BytesWritable();
    }

    /**
     * 读取数据
     * 返回值boolean  类型，如果返回true，表示文件已经读取完成，不用再继续往下读取了
     * 如果返回false，文件没有读取完成，继续读取下一行
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!flag) {
            long length = fileSplit.getLength();
            byte[] bytes = new byte[(int) length];
            Path path = fileSplit.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);
            FSDataInputStream fsDataInputStream = fileSystem.open(path);
            IOUtils.readFully(fsDataInputStream, bytes, 0, (int) length);
            bytesWritable.set(bytes, 0, (int) length);
            flag = true;
            return true;
        }
        return false;
    }

    /**
     * 获取数据的key1
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * 获取数据的value1
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    /**
     * 读取文件的进度，没什么用
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return flag ? 1.0f : 0.0f;
    }

    /**
     * 关闭资源
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {

    }
}
