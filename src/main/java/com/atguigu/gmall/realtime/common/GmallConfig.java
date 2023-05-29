package com.atguigu.gmall.realtime.common;

public class GmallConfig {
    //Phoenix 库名
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";
    //Phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //Phoenix 连接参数
    public static final String PHOENIX_SERVER =
            "jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181";
    //ClickHouse_url
    public static final String
            CLICKHOUSE_URL="jdbc:clickhouse://hadoop1:8123/default";
    //ClickHouse_Driver
    public static final String CLICKHOUSE_DRIVER =
            "ru.yandex.clickhouse.ClickHouseDriver";
}
