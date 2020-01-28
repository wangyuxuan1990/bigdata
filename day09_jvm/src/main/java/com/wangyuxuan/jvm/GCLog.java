package com.wangyuxuan.jvm;

/**
 * @author wangyuxuan
 * @date 2020/1/28 4:07 下午
 * @description 设置不同参数，观看GC日志
 * -Xmx20m -Xms20m -Xmn1m -XX:+PrintGCDetails
 * -Xmx20m -Xms20m -Xmn20m -XX:+PrintGCDetails
 * -Xmx20m -Xms20m -Xmn7m -XX:+PrintGCDetails
 * -Xmx20m -Xms20m -Xmn7m -XX:SurvivorRatio=2 -XX:+PrintGCDetails
 * -Xmx20m -Xms20m -XX:NewRatio=1 -XX:SurvivorRatio=2 -XX:+PrintGCDetails
 * -Xmx20m -Xms20m -XX:NewRatio=1 -XX:SurvivorRatio=4 -XX:+PrintGCDetails
 * -Xmx10m -Xms5m -XX:NewRatio=1 -XX:SurvivorRatio=4 -XX:+PrintGCDetails
 */
public class GCLog {
    public static void main(String[] args) {
        byte[] b = null;
        for (int i = 0; i < 10; i++) {
            b = new byte[1 * 1024 * 1024];
        }
    }
}
