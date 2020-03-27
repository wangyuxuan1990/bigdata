package com.wangyuxuan.stack;

/**
 * @author wangyuxuan
 * @date 2020/1/27 5:08 下午
 * @description 局部变量复用slot还伴随着可能会影响到系统垃圾收集的行为
 */
public class StackGc {
    // 示例一  垃圾没有被回收
//    public static void main(String[] args) {
//        // 向内存填充64M的数据
//        byte[] placeholder = new byte[64 * 1024 * 1024];
//        // 手动调用GC程序
//        System.gc();
//    }

    // 示例二  垃圾没有被回收
//    public static void main(String[] args) {
//        // 向内存填充64M的数据
//        {
//            byte[] placeholder = new byte[64 * 1024 * 1024];
//        }
//        // 手动调用GC程序
//        System.gc();
//    }

    // 示例三  垃圾被回收
    public static void main(String[] args) {
        // 向内存填充64M的数据
        {
            byte[] placeholder = new byte[64 * 1024 * 1024];
        }
        int a = 0;
        // 手动调用GC程序
        System.gc();
    }
}
