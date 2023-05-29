package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DImUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    //value:{
    //  "sinkTable" : "dim_base_trademark",
    //  "database"  : "gmall-flink",
    //  "before" : {
    //              "tm_name" ： “atguigu",
    //              "id" ： 12
    //          },
    //  "after" : {
    //              "tm_name" ： “Atguigu",
    //              "id" ： 12
    //          }
    // }
    //  "type" : "update"
    //  "tablename" : "base_trademark"
    // }
    //SQL : upsert into db.tn(id,tm_name) values(...,...)
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            //TODO 1.获取SQL语句
            String sinkTable = value.getString("sinkTable").toUpperCase();
            JSONObject after = value.getJSONObject("after");
            String upsertSql = genUpertSql(sinkTable,
                    after);
            System.out.println(upsertSql);
            //TODO 2.预编译SQL

            preparedStatement = connection.prepareStatement(upsertSql);
            //TODO 如果发现当前操作是更新操作，需要删除Redis中的数据
            //TODO 如果有这么一个维度 18 的数据，没用到所以没加载进redis，这时候 18 被更新了，
            // redis删除一个不存在的数据是不会报错的
            // 删除Redis的旧数据
            if(value.getString("type").equals("update")){
                DImUtil.delRedisDimInfo(
                        sinkTable.toUpperCase(),
                        after.getString("id"));
            }
            //TODO 3.执行插入操作
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(preparedStatement != null) preparedStatement.close();
        }
    }

    private String genUpertSql(String sinkTable, JSONObject after) {
        Set<String> keyset = after.keySet();
        Collection<Object> values = after.values();
        //注意以下是以 ',' 分割，因为插入是应该是 values （ '...','...','....' )
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(keyset,",") + ") values ('" +
                StringUtils.join(values,"','") + "')";
    }
}
