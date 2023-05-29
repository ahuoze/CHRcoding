package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface DimAsyncJoinFunction<T> {
    public  String getKey(T t);
    public  void join(T t, JSONObject dimInfo) throws ParseException;
}
