package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private OutputTag<JSONObject> outputTag;
    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Properties properties = new Properties();
        properties.put("phoenix.schema.isNamespaceMappingEnabled","true");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER,properties);
    }
    // s : { "db" : "","tn" : "" ,"before" : {} ,"after" : {},"type";""}
    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        //readOnlyContext,负责读数据
        //TODO 1.获取状态更新
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String key = jsonObject.getString("tablename") + "-" + jsonObject.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if(tableProcess != null){
            JSONObject after = jsonObject.getJSONObject("after");
            //TODO 2.过滤不需要的字段
            filterColumn(after,tableProcess.getSinkColumns());
            //TODO 3.分流 将输出主题信息写入jsonObject
            jsonObject.put("sinkTable",tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();

            if(TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                collector.collect(jsonObject);
            }else if(TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
                readOnlyContext.output(outputTag,jsonObject);
            }
        }else {
            System.out.println("该组合key：" + key + "不存在");
        }


    }

    /**
     *
     * @param after             {"id" : "11","tm_name" : "atguigu","logo_url" : "aaa"}
     * @param sinkColumns       id , tm_name
     */
    private void filterColumn(JSONObject after, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);

        Iterator<Map.Entry<String, Object>> iterator = after.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            if (!columns.contains(next.getKey())) {
                iterator.remove();
            }
        }

//        after.entrySet().removeIf(field -> !columns.contains(field));
    }

    // s : { "db" : "","tn" : "" ,"before" : {} ,"after" : {},"type";""}
    @Override
    public void processBroadcastElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
        //context,负责写数据
        //TODO 1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(s);
        String after = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(after, TableProcess.class);
        //TODO 2.建表
        if(TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }
        //TODO 3.写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key,tableProcess);
    }
    //建表语句： Create Table if not exists db.tn(id varchar primary key,tm_name varchar) xxx;
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) throws SQLException {
        PreparedStatement preparedStatement = null;
        try {
            sinkPk = sinkPk == null ? "id" : sinkPk;
            sinkExtend = sinkExtend == null ? "" : sinkExtend;

            StringBuffer createTableSQL = new StringBuffer("create Table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] fields = sinkColumns.split(",");
            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];
                if(sinkPk.equals(field)){
                    createTableSQL.append(field).append(" varchar primary key");
                }else {
                    createTableSQL.append(field).append(" varchar");
                }

                if(i < fields.length - 1) createTableSQL.append(",");
            }
            createTableSQL.append(")").append(sinkExtend);
            System.out.println(createTableSQL);
            //预编译SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());

            //执行
            preparedStatement.execute();
        } catch (SQLException e) {
            throw e;
//            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败");
        }finally {
            if(preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
