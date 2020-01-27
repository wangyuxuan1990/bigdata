package com.wangyuxuan.stack;

/**
 * @author wangyuxuan
 * @date 2020/1/27 4:21 下午
 * @description StackOverflowError栈溢出
 */
public class StackException {

    int i = 0;

    public static void main(String[] args) {
        StackException stackException = new StackException();
        stackException.stackExcept();
    }

    public void stackExcept() {
        i++;
        System.out.println("入栈第" + i + "次");
        stackExcept();
    }
}
