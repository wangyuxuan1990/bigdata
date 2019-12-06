package com.wangyuxuan.mr.demo10;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author wangyuxuan
 * @date 2019/12/6 16:40
 * @description OrderBean
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;

    private Double price;

    @Override
    public int compareTo(OrderBean o) {

        int orderIdCompare = this.orderId.compareTo(o.orderId);

        if (orderIdCompare == 0) {
            int priceCompare = this.price.compareTo(o.price);
            return -priceCompare;
        } else {
            return orderIdCompare;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.price = in.readDouble();
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + '\t' + price;
    }
}
