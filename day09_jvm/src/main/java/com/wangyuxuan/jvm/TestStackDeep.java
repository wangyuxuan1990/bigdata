package com.wangyuxuan.jvm;

/**
 * @author wangyuxuan
 * @date 2020/1/28 7:14 下午
 * @description 调整每个线程栈空间的大小  -Xss
 */
public class TestStackDeep {
    private static int count = 0;

    public static void recursion(long a, long b, long c) {
        long e = 1, f = 2, g = 3, h = 4, i = 5, k = 6, q = 7, x = 8, y = 9, z = 10;
        count++;
        recursion(a, b, c);
    }

    public static void main(String args[]) {
        try {
            recursion(0L, 0L, 0L);
        } catch (Throwable e) {
            System.out.println("deep of calling = " + count);
            e.printStackTrace();
        }
    }
}
