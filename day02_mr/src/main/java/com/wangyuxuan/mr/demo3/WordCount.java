package com.wangyuxuan.mr.demo3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author wangyuxuan
 * @date 2019/12/4 17:41
 * @description CombineTextInputFormat实战
 */
public class WordCount extends Configured implements Tool {

    /**
     * 实现Tool接口之后，需要实现一个run方法，
     * 这个run方法用于组装我们的程序的逻辑，其实就是组装八个步骤
     * 第一步：读取文件，解析成key,value对，k1   v1
     * 第二步：自定义map逻辑，接受k1   v1  转换成为新的k2   v2输出
     * 第三步：分区。相同key的数据发送到同一个reduce里面去，key合并，value形成一个集合
     * 第四步：排序   对key2进行排序。字典顺序排序
     * 第五步：规约  combiner过程  调优步骤 可选
     * 第六步：分组
     * 第七步：自定义reduce逻辑接受k2   v2  转换成为新的k3   v3输出
     * 第八步：输出k3  v3 进行保存
     *
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = super.getConf();
        Job job = Job.getInstance(configuration, "mrdemo3");
        job.setJarByClass(WordCount.class);

        job.setInputFormatClass(CombineTextInputFormat.class);
        // 虚拟存储切片最大值设置4m
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    /**
     * 作为程序的入口类
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // 提交run方法之后，得到一个程序的退出状态码
        int run = ToolRunner.run(configuration, new WordCount(), args);
        // 根据我们程序的退出状态码，退出整个进程
        System.exit(run);
    }
}
