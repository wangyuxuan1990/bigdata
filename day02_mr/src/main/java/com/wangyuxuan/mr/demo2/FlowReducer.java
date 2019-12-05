package com.wangyuxuan.mr.demo2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/5 10:53
 * @description reducer类
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean flowBean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        flowBean = new FlowBean();
    }

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int upFlow = 0;
        int downFlow = 0;
        int upCountFlow = 0;
        int downCountFlow = 0;
        for (FlowBean flowBean : values) {
            upFlow += flowBean.getUpFlow();
            downFlow += flowBean.getDownFlow();
            upCountFlow += flowBean.getUpCountFlow();
            downCountFlow += flowBean.getDownCountFlow();
        }
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);
        context.write(key, flowBean);
    }
}
