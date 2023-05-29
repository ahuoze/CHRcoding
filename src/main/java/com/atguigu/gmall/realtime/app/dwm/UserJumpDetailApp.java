package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.myKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

//数据流 web/app -> Nginx -> SpringBoot -> kafka(ods) -> flinkapp -> kafka(dwd) -> FlinkApp -> kafka(dwm)
//程序 mocklog -> Nginx -> Logger.sh -> kafka(ZK) -> BaseLogAPP -> kafka -> UserJumpDetailApp -> kafka
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//和kafka分区数保持一致
        //TODO 2.读取kafka主题数据创建流
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(myKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将每行数据转换为JSON对象并提取时间戳生产watermark
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSONObject::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts");
                                    }
                                }
                        ));

        //TODO 4.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastpageID = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastpageID == null || lastpageID.length() <= 0;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastpageID = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastpageID == null || lastpageID.length() <= 0;
            }
        }).within(Time.seconds(10));
        //TODO 使用循环模式 定义模式序列
        Pattern<JSONObject, JSONObject> pattern1 = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastpageID = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastpageID == null || lastpageID.length() <= 0;
            }
        }).times(2).consecutive().within(Time.seconds(10));
        //TODO 5.将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(
                jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid")),
                pattern);

        //TODO 6.提取匹配事件和超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("time-out") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOutTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0);
                    }
                }, new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0);
                    }
                });
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);

        //TODO 7.UNION两种事件
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        //TODO 8.将数据写入kafka
        unionDS.print();
        unionDS.map(JSONObject::toString).addSink(myKafkaUtil.getKafkaProducer(sinkTopic));
        //TODO 9.启动任务
        env.execute("UserJumpDetailApp");
    }
}
