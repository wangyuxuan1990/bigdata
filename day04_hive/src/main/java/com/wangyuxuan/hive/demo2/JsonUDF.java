package com.wangyuxuan.hive.demo2;

import com.google.gson.Gson;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author wangyuxuan
 * @date 2019/12/27 11:29
 * @description Json数据解析UDF开发
 */
public class JsonUDF extends UDF {

    public Movie evaluate(final Text text) {
        if (null == text) {
            return null;
        }
        Gson gson = new Gson();
        return gson.fromJson(text.toString(), Movie.class);
    }
}
