package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.bean.TransientSink;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    public static <T> SinkFunction<T> getSink(String sql){
        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        try {
                            //获取所有属性信息
                            Field[] declaredFields = t.getClass().getDeclaredFields();
                            int offset = 0;
                            for (int i = 0; i < declaredFields.length; i++) {
                                //获取字段对象

                                Field field = declaredFields[i];
                                field.setAccessible(true);
                                //获取字段注解
                                TransientSink annotation = field.getAnnotation(TransientSink.class);
                                if(annotation != null){
                                    //存在注解，不写出去
                                    offset++;
                                    continue;
                                }
                                //获取值
                                Object o = null;
                                o = field.get(t);
                                //给预编译对象赋值
                                preparedStatement.setObject(i + 1 - offset,o.toString());
                            }
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withUsername("default")
                        .withPassword("123456")
                        .build()
        );
    }
}
