package com.wangyuxuan.jvm;

import java.util.Vector;

/**
 * @author wangyuxuan
 * @date 2020/1/28 5:14 下午
 * @description 查看GC错误日志  -Xmx20m -Xms5m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/a.dump
 */
public class GCDump {
    public static void main(String[] args) {
        Vector v = new Vector();
        for (int i = 0; i < 25; i++) {
            v.add(new byte[1 * 1024 * 1024]);
        }
    }
}
