package com.wangyuxuan.mr.demo10;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author wangyuxuan
 * @date 2019/12/6 16:55
 * @description 自定义GroupingComparator
 */
public class MyGroup extends WritableComparator {

    /**
     * compare方法接受到两个参数，这两个参数其实就是我们前面传过来的OrderBean
     *
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean first = (OrderBean) a;
        OrderBean second = (OrderBean) b;
        return first.getOrderId().compareTo(second.getOrderId());
    }

    /**
     * 覆写默认构造器
     * 通过反射来构造OrderBean对象
     * 接受到的key2  是orderBean类型，我们就需要告诉分组，以orderBean接受我们的参数
     */
    public MyGroup() {
        super(OrderBean.class, true);
    }
}
