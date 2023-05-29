package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.Bean1;
import com.atguigu.bean.Bean2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class FlinkDataStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Bean1> stream1 = env.socketTextStream("hadoop1", 8888)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean1(fields[0], fields[1], Long.parseLong(fields[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean1>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Bean1>() {
                            @Override
                            public long extractTimestamp(Bean1 bean1, long l) {
                                return bean1.getTs() * 1000L;
                            }
                        }));
        SingleOutputStreamOperator<Bean2> stream2 = env.socketTextStream("hadoop1", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean2(fields[0], fields[1], Long.parseLong(fields[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean2>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Bean2>() {
                            @Override
                            public long extractTimestamp(Bean2 bean2, long l) {
                                return bean2.getTs() * 1000L;
                            }
                        }));
        SingleOutputStreamOperator<Tuple2<Bean1, Bean2>> joinDS = stream1.keyBy(Bean1::getIo).intervalJoin(stream2.keyBy(Bean2::getIo))
                .between(Time.seconds(-5), Time.seconds(5))
//                .upperBoundExclusive()    加上后 右開
//                .lowerBoundExclusive()    加上後 左開
                .process(
                        new ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>() {
                            @Override
                            public void processElement(Bean1 bean1, Bean2 bean2, Context context, Collector<Tuple2<Bean1, Bean2>> collector) throws Exception {
                                collector.collect(new Tuple2<>(bean1, bean2));
                            }
                        }
                );
        joinDS.print();
        env.execute();
    }
}
