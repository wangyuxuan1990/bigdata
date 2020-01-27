package com.wangyuxuan.stack;

/**
 * @author wangyuxuan
 * @date 2020/1/27 5:23 下午
 * @description 方法的参数对调用次数的影响
 */
public class MethodParameter {
    private static int count = 0;

    public static void recursion(int a, int b, int c) {
        System.out.println("我是有参数的方法，调用第" + count + "次");
        long l1 = 12;
        short sl = 1;
        byte b1 = 1;
        String s = "1";
        count++;
        recursion(1, 2, 3);
    }

    public static void recursion() {
        System.out.println("我是没有参数的方法，调用第" + count + "次");
        count++;
        recursion();
    }

    public static void main(String[] args) {
//        recursion();  //调用第6685次
        recursion(1, 2, 3);  //调用第6076次
    }
}
