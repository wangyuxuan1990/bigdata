package com.wangyuxuan.mr.demo11;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/7 2:32 下午
 * @description 自定义OutputFormat
 */
public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        Path goodComment = new Path("file:///good.txt");
        Path badComment = new Path("file:///bad.txt");
        FSDataOutputStream goodOutStream = fileSystem.create(goodComment);
        FSDataOutputStream badOutStream = fileSystem.create(badComment);
        return new MyOutputFormatWriter(goodOutStream, badOutStream);
    }

    static class MyOutputFormatWriter extends RecordWriter<Text, NullWritable> {

        FSDataOutputStream goodStream = null;
        FSDataOutputStream badStream = null;

        public MyOutputFormatWriter(FSDataOutputStream goodStream, FSDataOutputStream badStream) {
            this.goodStream = goodStream;
            this.badStream = badStream;
        }

        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            if ("0".equals(key.toString().split("\t")[9])) {
                goodStream.write(key.toString().getBytes());
                goodStream.write("\r\n".getBytes());
            } else {
                badStream.write(key.toString().getBytes());
                badStream.write("\r\n".getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (goodStream != null) {
                goodStream.close();
            }

            if (badStream != null) {
                badStream.close();
            }
        }
    }
}
