package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.myKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

//数据流 web/app -> Nginx -> SpringBoot -> kafka(ods) -> flinkapp -> kafka(dwd) -> flinkApp -> kafka(dwm)
//程序 mocklog -> Nginx -> Logger.sh -> kafka(ZK) -> BaseLogAPP -> kafka -> UniqueVisitApp -> kafka

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.读取kafka dwd_page_log 数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(myKafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        //TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSONObject::parseObject);
        /*
        TODO 4.过滤数据 状态编程 仅保留每天用户第一条登录数据
        {"common":{"ar":"440000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone Xs","mid":"mid_7","os":"iOS 13.3.1",
        "uid":"20","vc":"v2.0.1"},"page":{"during_time":13385,"last_page_id":"good_detail","page_id":"cart"},"ts":1608279683000}

        */

        SingleOutputStreamOperator<JSONObject> uvDS = jsonObjDS
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> dateState;
            private SimpleDateFormat simpleDateFormat;
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<String>("date-state",String.class);
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);
                dateState = getRuntimeContext().getState(stringValueStateDescriptor);

                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if(lastPageId == null || lastPageId.length() <= 0){
                    String lastDate = dateState.value();

                    String curDate = simpleDateFormat.format(jsonObject.getLong("ts"));

                    if(!curDate.equals(lastDate)){
                        dateState.update(curDate);
                        return true;
                    }
                }
                return false;
            }
        });
        //TODO 5.将数据写入kafka dwm层
        uvDS.print();
        uvDS.map(JSONObject::toString).addSink(myKafkaUtil.getKafkaProducer(sinkTopic));
        //TODO 6.启动任务
        env.execute("UniqueVisitApp");

    }
}
