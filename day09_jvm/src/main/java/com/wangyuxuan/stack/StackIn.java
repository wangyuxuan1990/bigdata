package com.wangyuxuan.stack;

/**
 * @author wangyuxuan
 * @date 2020/1/27 4:42 下午
 * @description 虚拟机栈运行原理
 */
public class StackIn {
    // 程序入口方法
    public static void main(String[] args) {
        StackIn stackIn = new StackIn();
        // 调用A 方法，产生了栈帧 F1
        stackIn.A();
    }

    // 最后弹出F1
    public void A() {
        System.out.println("A");
        // F1栈帧里面调用B方法，产生栈帧F2
        B();
    }

    // 然后弹出F2
    public void B() {
        // F2栈帧里面调用C方法，产生栈帧F3
        System.out.println("B");
        C();
    }

    // 栈帧F3  执行完成之后，先弹出F3
    public void C() {
        System.out.println("C");
    }
}
