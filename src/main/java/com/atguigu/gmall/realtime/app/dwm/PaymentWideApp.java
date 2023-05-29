package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentInfo;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.utils.myKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

//数据流：web/app -> nginx -> mysql -> flinkApp -> kafka(ods) -> flinkapp
//          -> kafka/hbase(dwd,dim) -> flinkapp(redis) -> kakfa(dwm) -> flinkapp -> kafka(dwm)
//程序 ：mockDB -> Mysql -> FlinkCDC -> kafka(zk) -> BaseDBApp -> Kafka/Phoenix(zk/hdfs/hbase)
//      -> OrderWidedApp(Redis) -> Kafka -> PaymentWideApp -> Kafka
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.读取Kafka主题的数据创建流 并转换为Java Bean对象 同时提取时间戳生成WaterMark
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        SingleOutputStreamOperator<OrderWide> orderWideDS = env.addSource(myKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
                .map(line -> JSONObject.parseObject(line, OrderWide.class)).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                                    @Override
                                    public long extractTimestamp(OrderWide orderWide, long l) {
                                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                        try {
                                            return sdf.parse(orderWide.getCreate_time()).getTime();
                                        } catch (ParseException e) {
                                            e.printStackTrace();
                                            //l 代表进入系统的时间；
                                            return l;
                                        }
                                    }
                                }));
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = env.addSource(myKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
                .map(line -> JSONObject.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                        try {
                                            return sdf.parse(paymentInfo.getCreate_time()).getTime();
                                        } catch (ParseException e) {
                                            e.printStackTrace();
                                            //l 代表进入系统的时间；
                                            return l;
                                        }
                                    }

                                }));
        //TODO 3.双流Join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<PaymentWide> collector) throws Exception {
                        collector.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //TODO 4.数据写入Kafka
        paymentWideDS.print("patmentWideDS>>>>");
        paymentWideDS.map(JSONObject::toJSONString)
                .addSink(myKafkaUtil.getKafkaProducer(paymentWideSinkTopic));
        //TODO 5.启动任务
        env.execute("paymentWideApp");
    }
}