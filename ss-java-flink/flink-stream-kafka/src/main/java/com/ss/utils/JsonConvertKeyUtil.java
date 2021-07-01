package com.ss.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.hadoop.shaded.com.google.common.base.CaseFormat;

import java.util.Set;

/**
 * Created by zhaozh on 2021/07/01.
 */
public class JsonConvertKeyUtil {

    public static JSONObject covertObjectToString(JSONObject object) {
        if (object == null) {
            return null;
        }
        JSONObject newObject = new JSONObject();
        Set<String> set = object.keySet();
        for (String key : set) {
            Object value = object.get(key);
            if (value instanceof JSONArray) {
                value = value.toString();
            }
            // 这个方法自己写的改成驼峰，也可以改成大写小写
            key = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, key);
            newObject.put(key, value);
        }
        return newObject;
    }

    private JSONObject covertObject(JSONObject object) {
        if (object == null) {
            return null;
        }
        JSONObject newObject = new JSONObject();
        Set<String> set = object.keySet();
        for (String key : set) {
            Object value = object.get(key);
            if (value instanceof JSONArray) {
                // 数组
                value = covertArray(object.getJSONArray(key));
            } else if (value instanceof JSONObject) {
                // 对象
                value = covertObject(object.getJSONObject(key));
            }
            // 这个方法自己写的改成驼峰，也可以改成大写小写
            key = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, key);
            newObject.put(key, value);
        }
        return newObject;
    }

    private JSONArray covertArray(JSONArray array) {
        if (array == null) {
            return null;
        }
        JSONArray newArray = new JSONArray();
        for (int i = 0; i < array.size(); i++) {
            Object value = array.get(i);
            if (value instanceof JSONArray) {
                // 数组
                value = covertArray(array.getJSONArray(i));
            } else if (value instanceof JSONObject) {
                // 对象
                value = covertObject(array.getJSONObject(i));
            }
            newArray.add(value);
        }
        return newArray;
    }

    public String convertJSONKeyRetrunString(String jsonStr) {
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        JSONObject jsonResult = this.covertObject(jsonObject);
        return jsonResult.toJSONString();
    }
}
