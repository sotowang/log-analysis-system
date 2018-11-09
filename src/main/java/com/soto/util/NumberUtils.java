package com.soto.util;

import java.math.BigDecimal;

/**
 * 数字格式工具类
 */
public class NumberUtils {

    /**
     * 格式化小数
     * @param num
     * @param scale
     * @return
     */
    public static double formatDouble(double num, int scale) {
        BigDecimal bigDecimal = new BigDecimal(num);
        return bigDecimal.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    public static void main(String[] args) {
        System.out.println(formatDouble(10.24, 1));
    }
}
