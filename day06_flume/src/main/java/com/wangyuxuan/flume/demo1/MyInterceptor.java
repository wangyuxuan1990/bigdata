package com.wangyuxuan.flume.demo1;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wangyuxuan
 * @date 2020/1/10 10:19
 * @description flume自定义拦截器
 */
public class MyInterceptor implements Interceptor {
    /**
     * encrypted_field_index. 指定需要加密的字段下标
     */
    private final String encrypted_field_index;


    /**
     * The out_index. 指定不需要对应列的下标
     */
    private final String out_index;

    /**
     * 提供构建方法，后期可以接受配置文件中的参数
     *
     * @param encrypted_field_index
     * @param out_index
     */
    public MyInterceptor(String encrypted_field_index, String out_index) {
        this.encrypted_field_index = encrypted_field_index.trim();
        this.out_index = out_index.trim();
    }

    @Override
    public void initialize() {

    }

    /**
     * 单个event拦截逻辑
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        if (event == null) {
            return null;
        }
        try {
            String line = new String(event.getBody(), Charsets.UTF_8);
            String[] fields = line.split(",");

            String newLine = "";
            for (int i = 0; i < fields.length; i++) {
                // 字符串数字转成int
                int encryptedField = Integer.parseInt(encrypted_field_index);
                int outIndex = Integer.parseInt(out_index);
                if (i == encryptedField) {
                    newLine += md5(fields[i]) + ",";
                } else if (i != outIndex) {
                    newLine += fields[i] + ",";
                }
            }
            newLine = newLine.substring(0, newLine.length() - 1);
            event.setBody(newLine.getBytes(Charsets.UTF_8));
            return event;
        } catch (Exception e) {
            return event;
        }
    }

    /**
     * 批量event拦截逻辑
     *
     * @param list
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> out = new ArrayList<>();
        for (Event event : list) {
            Event outEvent = intercept(event);
            if (outEvent != null) {
                out.add(outEvent);
            }
        }
        return out;
    }

    @Override
    public void close() {

    }

    /**
     * 写一个md5加密的方法
     *
     * @param plainText
     * @return
     */
    public static String md5(String plainText) {
        // 定义一个字节数组
        byte[] secretBytes = null;
        try {
            // 生成一个MD5加密计算摘要
            MessageDigest md = MessageDigest.getInstance("MD5");
            // 对字符串进行加密
            md.update(plainText.getBytes());
            // 获得加密后的数据
            secretBytes = md.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("没有md5这个算法！");
        }
        // 将加密后的数据转换为16进制数字
        // 16进制数字
        String md5code = new BigInteger(1, secretBytes).toString(16);
        // 如果生成数字未满32位，需要前面补0
        for (int i = 0; i < 32 - md5code.length(); i++) {
            md5code = "0" + md5code;
        }
        return md5code;
    }

    /**
     * 相当于自定义Interceptor的工厂类
     * 在flume采集配置文件中通过制定该Builder来创建Interceptor对象
     * 可以在Builder中获取、解析flume采集配置文件中的拦截器Interceptor的自定义参数：
     * 指定需要加密的字段下标 指定不需要对应列的下标等
     */
    public static class MyBuilder implements Interceptor.Builder {
        /**
         * encrypted_field_index. 指定需要加密的字段下标
         */
        private String encrypted_field_index;


        /**
         * The out_index. 指定不需要对应列的下标
         */
        private String out_index;

        @Override
        public Interceptor build() {
            return new MyInterceptor(encrypted_field_index, out_index);
        }

        @Override
        public void configure(Context context) {
            encrypted_field_index = context.getString("encrypted_field_index", "");
            out_index = context.getString("out_index", "");
        }
    }
}
