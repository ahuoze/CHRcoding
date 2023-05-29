package com.atguigu.gmall.realtime.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
//新引入的时间日期API
//TODO LocalDate代表年月日，LocalTime代表时分秒，LocalDateTime代表年月日时分秒
//
public class DateTimeUtil {
    private final static DateTimeFormatter formator =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static String toYMDhms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(),
                ZoneId.systemDefault());
        return formator.format(localDateTime);
    }
    public static Long toTs(String YmDHms) {
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, formator);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
}