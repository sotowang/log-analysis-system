package com.soto.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 参数工具类
 */
public class ParamUtils {

    /**
     * 从命令行参数中提取任务ID
     * @param args
     * @return
     */
    public static Long getTaskIdFromArgs(String[] args) {
        if (args != null && args.length > 0) {
            return Long.valueOf(args[0]);
        }
        return null;
    }

    /**
     * 从Json中提取参数
     * @param jsonObject
     * @param field
     * @return
     */
    public static String getParam(JSONObject jsonObject, String field) {
        JSONArray jsonArray = jsonObject.getJSONArray(field);
        if (jsonArray != null && jsonArray.size() > 0) {
            return jsonArray.getString(0);
        }
        return null;
    }
}
