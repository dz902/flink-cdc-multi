package org.example.utils;

import com.alibaba.fastjson.JSONObject;

public class JSONUtils {
    public static JSONObject objectToJSONObject(Object object) {
        return JSONObject.parseObject(
            JSONObject.toJSONString(object)
        );
    }
}
