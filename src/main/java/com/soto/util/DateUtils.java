package com.soto.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间日期工具类
 */
public class DateUtils {
    public static final SimpleDateFormat TIME_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat DATE_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd");
    public static final SimpleDateFormat DATEKEY_FORMAT =
            new SimpleDateFormat("yyyyMMdd");

    /**
     * 判断时间time1 是否在 time2 之前，是返回true，否则为false
     * @param time1
     * @param time2
     * @return
     */
    public static boolean before(String time1, String time2) {
        try {
            Date dateTime1 = TIME_FORMAT.parse(time1);
            Date dateTime2 = TIME_FORMAT.parse(time2);

            if (dateTime1.before(dateTime2)) {
                return true;
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return false;
    }

    /**
     * 判断时间time1 是否在 time2 之后，是返回true，否则为false
     * @param time1
     * @param time2
     * @return
     */
    public static boolean after(String time1, String time2) {
        try {
            Date dateTime1 = TIME_FORMAT.parse(time1);
            Date dateTime2 = TIME_FORMAT.parse(time2);

            if (dateTime1.after(dateTime2)) {
                return true;
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return false;
    }

    /**
     * 计算时间差
     * @param time1
     * @param time2
     * @return
     */
    public static int minus(String time1, String time2) {
        try {
            Date dateTime1 = TIME_FORMAT.parse(time1);
            Date dateTime2 = TIME_FORMAT.parse(time2);

            long millsecond = dateTime1.getTime() - dateTime2.getTime();
            return Integer.valueOf(String.valueOf(millsecond / 1000));

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取年月日和小时
     * @param datetime  （yyyy-MM-dd HH:mm:ss）
     * @return  (yyyy-MM-dd_HH)
     */
    public static String getDateHour(String datetime) {
        String date = datetime.split(" ")[0];
        String hourMinuteSecond = datetime.split(" ")[1];
        String hour = hourMinuteSecond.split(":")[0];
        return date + "_" + hour;


    }

    /**
     * 获取当天日期  2018-11-09
     * @return
     */
    public static String getTodayDate() {
        return DATE_FORMAT.format(new Date());
    }

    /**
     * 获取昨天日期  2018-11-08
     * @return
     */
    public static String getYesterdayDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        Date date = calendar.getTime();

        return DATE_FORMAT.format(date);
    }

    /**
     * 格式化日期
     * @param date
     * @return
     */
    public static String formatDate(Date date) {
        return DATE_FORMAT.format(date);
    }

    /**
     * 格式化时间  2018-11-09 15:30:40
     * @param date
     * @return
     */
    public static String formatTime(Date date) {
        return TIME_FORMAT.format(date);
    }

    /**
     * 解析时间字符串
     * @param time
     * @return
     */
    public static Date parseTime(String time) {
        try {
            return TIME_FORMAT.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 格式化日期key
     * @param date
     * @return
     */
    public static String formatDateKey(Date date) {
        return DATEKEY_FORMAT.format(date);
    }

    public static String formatTimeMinute(Date date) {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm");
        return simpleDateFormat.format(date);
    }


    public static void main(String[] args) {
        System.out.println(getDateHour("2018-11-11 13:55:27"));
        System.out.println(getTodayDate());
        System.out.println(getYesterdayDate());
        System.out.println(formatDate(new Date()));
        System.out.println(formatTime(new Date()));
        System.out.println(new Date());
        System.out.println(parseTime("2018-11-11 13:55:27"));
        System.out.println(formatTimeMinute(new Date()));
    }

}
