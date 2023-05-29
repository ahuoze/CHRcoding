package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class DImUtil {
    public static JSONObject getDimInfo(Connection connection,String tableName,String id) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException {
        //查询Phoenix之前查询Redis
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);
        if(dimInfoJsonStr != null){

            //重置过期时间
            jedis.expire(redisKey,24 * 60 * 60);
            jedis.close();
//            System.out.println(dimInfoJsonStr);
            return JSONObject.parseObject(dimInfoJsonStr.substring(1,dimInfoJsonStr.length() - 1));
        }
        //拼接sqL
        String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName
                + " where id= '" + id + "'";
        //查询Phoenix
        List<JSONObject> jsonObjects = JDBCUtil.queryList(connection, sql, JSONObject.class, false);


        JSONObject dimInfoJson = jsonObjects.get(0);

        //查到Phoenix后，写入redis
        jedis.set(redisKey,jsonObjects.toString());
        jedis.expire(redisKey,24 * 60 * 60);
        jedis.close();

        //虽然是一个列表，但是限死了id，只会找到一个值。
        return dimInfoJson;

    }

    public static void delRedisDimInfo(String tablename,String id){
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tablename + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        System.out.println((getDimInfo(connection,"DIM_BASE_PROVINCE","23")));
        connection.close();
    }
 }
