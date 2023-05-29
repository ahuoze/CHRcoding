package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.myKafkaUtil;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.ParseException;

//数据流 web/app -> Nginx -> SpringBoot -> kafka(ods) -> flinkapp -> kafka(dwd)
//程序 mocklog -> Nginx -> Logger.sh -> kafka(ZK) -> BaseLogAPP -> kafka

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        //TODO 2.消费ods_base_log 主题数据创建流
        String topic = "ods_base_log";
        String groupID = "base_log_app";
        DataStreamSource<String> kafkaDS = executionEnvironment.addSource(myKafkaUtil.getKafkaConsumer(topic, groupID));
        //TODO 3.将每行数据转换为JSON
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    //发生解析异常，写入侧输出流
                    context.output(dirtyOutputTag, s);
                }

            }
        });
        jsonObjDS.getSideOutput(dirtyOutputTag).print("Dirty>>>>");
        //TODO 4.新老用户校验 状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlag = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("value-state", String.class)
                        );
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        //获取json中的"is_new"标记
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        if ("1".equals(isNew)) {
                            //获取状态数据

                            String value = valueState.value();
                            if (value != null) {
                                // 修改isNew标记
                                jsonObject.getJSONObject("common").put("is_New", "0");
                            } else {
                                valueState.update("1");
                            }
                        }
                        return jsonObject;
                    }
                });
        //TODO 5.分流 侧输出流 页面：主流 启动：侧输出流 曝光：侧输出流
        OutputTag<String> startOutputTag = new OutputTag<String>("start"){};
        OutputTag<String> displayOutTag = new OutputTag<String>("display"){};
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlag.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                //启动日志字段
                String start = jsonObject.getString("start");
                if (start != null && start.length() > 0) {
                    context.output(startOutputTag, jsonObject.toJSONString());
                } else {
                    collector.collect(jsonObject.toJSONString());
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //获取页面ID
                        String pageID = jsonObject.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            //添加页面id信息
                            display.put("page_id", pageID);
                            context.output(displayOutTag, display.toJSONString());

                        }
                    }
                }
            }
        });
        //TODO 6.提取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startOutputTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayOutTag);
        //TODO 7.将三个流进行打印并输出到对应的Kafka分区
        startDS.print("Start>>>>");
        pageDS.print("Page>>>>");
        displayDS.print("Display>>>>");

        startDS.addSink(myKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.addSink(myKafkaUtil.getKafkaProducer("dwd_page_log"));
        displayDS.addSink(myKafkaUtil.getKafkaProducer("dwd_display_log"));

        //TODO 8.启动任务
        executionEnvironment.execute("BaseLogApp");
    }
}
