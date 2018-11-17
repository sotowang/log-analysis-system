package com.soto.util;

/**
 * 字符串工具类
 */
public class StringUtils {

    /**
     * 判断字符串是否为空
     * @param string
     * @return
     */
    public static boolean isEmpty(String string) {
        return string == null || "".equals(string);
    }

    /**
     * 判断字符串是否不为空
     * @param string
     * @return
     */
    public static boolean isNotEmpty(String string) {
        return string != null && !"".equals(string);
    }

    /**
     * 截断字符串两端的逗号
     * @param str
     * @return
     */
    public static String trimComma(String str) {
        if (str.startsWith(",")) {
            str = str.substring(1);
        }
        if (str.endsWith(",")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }

    /**
     * 补全两位数字
     * @param str
     * @return
     */
    public static String fulfuill(String str) {
        if (str.length() == 2) {
            return str;
        }else {
            return "0" + str;
        }
    }

    /**
     * 从拼接的字符串中提取字段
     * @param str
     * @param delimiter
     * @param field
     * @return
     */
    public static String getFieldFromConcatString(String str,
                                                  String delimiter, String field) {
        try {
            String[] fields = str.split(delimiter);
            for(String concatField : fields) {
                // searchKeywords=|clickCategoryIds=1,2,3
                if(concatField.split("=").length == 2) {
                    String fieldName = concatField.split("=")[0];
                    String fieldValue = concatField.split("=")[1];
                    if(fieldName.equals(field)) {
                        return fieldValue;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String setFieldInConcatString(String str,
                                                String delimiter, String field, String newFieldValue) {
        String[] fields = str.split(delimiter);

        for (int i = 0; i < fields.length; i++) {
            String fieldName = fields[i].split("=")[0];
            if (fieldName.equals(field)) {
                String concatField = fieldName + "=" + newFieldValue;
                fields[i] = concatField;
                break;
            }
        }
        StringBuffer buffer = new StringBuffer("");
        for (int i = 0; i < fields.length; i++) {
            buffer.append(fields[i]);
            if (i < fields.length - 1) {
                buffer.append("|");
            }
        }
        return buffer.toString();
    }

    public static void main(String[] args) {
        System.out.println(trimComma(",sss,s,"));
        System.out.println(fulfuill("1"));
    }

}
