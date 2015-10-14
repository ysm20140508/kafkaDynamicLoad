package com.yunva.utill;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.parser.Feature;

/**
 * Created by Administrator on 2015-06-11.
 */
public class FastjsonUtills {
    /**
     * json
     *
     * @param v
     * @return
     */
    public static final byte[] toJSONString(Object v) {
        String json = JSON.toJSONString(v);
        return json.getBytes();
    }

    /**
     * json
     *
     * @param text
     * @param T
     * @param <T>
     * @return
     */
    public static final <T> T parseObject(byte[] text, Class<T> T) {
        return JSON.parseObject(text, T);
    }

    /**
     * json
     *
     * @param text
     * @param type
     * @param <T>
     * @return
     */
    public static final <T> T parseObject(byte[] text, TypeReference<T> type) {
        return JSON.parseObject(new String(text), type, Feature.AllowArbitraryCommas);
    }

}
