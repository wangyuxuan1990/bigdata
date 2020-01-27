package com.wangyuxuan.jvm;

/**
 * @author wangyuxuan
 * @date 2020/1/27 3:52 下午
 * @description 验证类加载器机制
 */
public class ClassLoaderTest {
    public static void main(String[] args) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        System.out.println(loader);
        System.out.println(loader.getParent());
        System.out.println(loader.getParent().getParent());
    }
}
