package com.wangyuxuan.mr.demo15;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wangyuxuan
 * @date 2019/12/11 11:53
 * @description mapper类
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> pdtsMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pdtsMap = new HashMap<>();
        Configuration configuration = context.getConfiguration();
        // 获取到所有的缓存文件，但是现在只有一个缓存文件
        URI[] cacheFiles = DistributedCache.getCacheFiles(configuration);
        // 获取到了我们放进去的缓存文件
        URI cacheFile = cacheFiles[0];
        // 获取FileSystem
        FileSystem fileSystem = FileSystem.get(cacheFile, configuration);
        // 读取文件，获取到输入流。这里面装的都是商品表的数据
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(cacheFile));
        // 获取到BufferedReader 之后就可以一行一行的读取数据
        BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream));
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] split = line.split(",");
            pdtsMap.put(split[0], line);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        context.write(new Text(value + "\t" + pdtsMap.get(split[2])), NullWritable.get());
    }
}
