package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.myKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple4;

import java.time.Duration;
import java.util.Date;

//数据流 web/app -> Nginx -> SpringBoot -> kafka(ods) -> flinkapp -> kafka(dwd) -> flinkapp -> kafka(dwm)
// -> flinkapp -> clickHousse
//程序 mocklog -> Nginx -> Logger.sh -> kafka(ZK) -> BaseLogAPP -> kafka -> uv/uj APP -> Kafka
//  -> VisitorStatsApp -> ClickHouse
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.读取Kafka数据创建流
        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        DataStreamSource<String> uvDS = env.addSource(myKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(myKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
        DataStreamSource<String> pvDS = env.addSource(myKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));

        //TODO 3.将每个流处理成相同的数据类型 维度信息 ar地区，ch渠道，is_new新老用户 ，vc版本号
        //TODO                              度量  uv唯一用户访问次数 pv页面访问量 uj页面跳出量 sv进入页面数 dur_sum 访问时长
        //TODO {"common":
        //          {"ar":"110000","ba":"Xiaomi","ch":"360","is_new":"0",
        //          "md":"Xiaomi 10 Pro ","mid":"mid_3","os":"Android 11.0","uid":"17","vc":"v2.1.134"},
        //      "start":{"entry":"icon","loading_time":9110,"open_ad_id":12,"open_ad_ms":7567,
        //      "open_ad_skip_ms":7245},"ts":1608281292000}
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts")
            );
        });
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts")
            );
        });
        //TODO{"common":{"ar":"110000","ba":"Xiaomi","ch":"360","is_new":"0","md":"Xiaomi 10 Pro ",
        //              "mid":"mid_3","os":"Android 11.0","uid":"17","vc":"v2.1.134"},
        //  "page":{"during_time":2489,"last_page_id":"home","page_id":"search"},"ts":1608281292000}
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvDS = pvDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);

            JSONObject common = jsonObject.getJSONObject("common");
            //获取上一条页面id
            JSONObject page = jsonObject.getJSONObject("page");
            String last_page_id = page.getString("last_page_id");
            long sv = 0L;
            if (last_page_id == null || last_page_id.length() <= 0) sv = 1L;
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L, page.getLong("during_time"),
                    jsonObject.getLong("ts")
            );

        });
        //TODO 4.Union几个流
        DataStream<VisitorStats> unionDS = visitorStatsWithUvDS.union(
                visitorStatsWithUjDS,
                visitorStatsWithPvDS);
        //TODO 5.提取时间戳生成WaterMark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats visitorStats, long l) {
                                return visitorStats.getTs();
                            }
                        }));
        //TODO 6.按照维度信息分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWMDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return new Tuple4<String, String, String, String>(
                        visitorStats.getAr(),
                        visitorStats.getCh(),
                        visitorStats.getIs_new(),
                        visitorStats.getVc()
                );
            }
        });

        //TODO 7.开窗聚合，开窗时间设置为10s
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<VisitorStats> result = windowDS.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats v1, VisitorStats v2) throws Exception {
//                new VisitorStats(v1.getStt(),
//                        v1.getEdt(),
//                        v1.getVc(),
//                        v1.getCh(),
//                        v1.getAr(),
//                        v1.getIs_new(),
//                        v1.getUv_ct() + v2.getUv_ct())
                v1.setUv_ct(v1.getUv_ct() + v2.getUv_ct());
                v1.setPv_ct(v1.getPv_ct() + v2.getPv_ct());
                v1.setSv_ct(v1.getSv_ct() + v2.getSv_ct());
                v1.setUj_ct(v1.getUj_ct() + v2.getUj_ct());
                v1.setDur_sum(v1.getDur_sum() + v2.getDur_sum());
                return v1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4,
                              TimeWindow timeWindow,
                              Iterable<VisitorStats> iterable,
                              Collector<VisitorStats> collector) throws Exception {
                long start = timeWindow.getStart();
                long end = timeWindow.getEnd();
                VisitorStats next = iterable.iterator().next();
                next.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                next.setEdt(DateTimeUtil.toYMDhms(new Date(end)));
                collector.collect(next);
            }
        });

        //TODO 8.数据写入ClickHouse
        result.print("result>>>>");
        result.addSink(
                ClickHouseUtil.getSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute("VisitorStatsApp");
    }
}
