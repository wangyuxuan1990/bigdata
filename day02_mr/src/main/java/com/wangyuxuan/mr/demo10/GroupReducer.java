package com.wangyuxuan.mr.demo10;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/6 17:03
 * @description reducer
 */
public class GroupReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 分组求Top1
//        context.write(key, NullWritable.get());

        // 分组求TopN

        // 需要对我们集合只输出两个值
        int i = 0;
        for (NullWritable value : values) {
            if (i < 2) {
                context.write(key, value);
                i++;
            } else {
                break;
            }

        }
    }
}
