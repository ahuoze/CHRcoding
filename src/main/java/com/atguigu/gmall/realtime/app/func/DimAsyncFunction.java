package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DImUtil;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimAsyncJoinFunction<T>{
    private final String tablename;
    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;


    public DimAsyncFunction(String tablename) {
        this.tablename = tablename;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getThreadPool();
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                //TODO 获取id字段
                String id = getKey(t);
                //TODO 查询维度信息
                JSONObject dimInfo = DImUtil.getDimInfo(connection, tablename, id);
                //TODO 补充维度信息
                if(dimInfo != null){
                    join(t,dimInfo);
                }
                //TODO 将数据输出
                resultFuture.complete(Collections.singletonList(t));
            }
        });
    }



    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);
    }
}
