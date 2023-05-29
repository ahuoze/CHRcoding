package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.bean.ProductStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.JDBCUtil;
import com.atguigu.gmall.realtime.utils.myKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStasApp {
    public static void main(String[] args) {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.读取Kafka7个主题的数据流
        String groupId = "product_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        DataStreamSource<String> pvDS = env.addSource(myKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> favorDS = env.addSource(myKafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId));
        DataStreamSource<String> cardDS = env.addSource(myKafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId));
        DataStreamSource<String> orderDS = env.addSource(myKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> payDS = env.addSource(myKafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId));
        DataStreamSource<String> refundDS = env.addSource(myKafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentDS = env.addSource(myKafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId));
        //TODO 3.将7个数据流同一数据格式
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pvDS.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String s, Collector<ProductStats> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                JSONObject page = jsonObject.getJSONObject("page");
                String page_id = page.getString("page_id");

                Long ts = jsonObject.getLong("ts");

                if ("good_detail".equals(page_id) && "sku_id".equals(page.getString("item_type"))) {
                    //说明这是一个点击数据
                    collector.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }
                //尝试取出曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        //取出单条曝光数据
                        JSONObject display = displays.getJSONObject(i);

                        if ("sku_id".equals(display.getString("item_type"))) {
                            //确保曝光的是一个商品而不是一个活动id activity_id之类的
                            collector.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });
        // 收藏表字段： {id , user_id , sku_id , spu_id , is_cancel , create_time , cancel_time}
        SingleOutputStreamOperator<ProductStats> productStatsWithFavoriteDS = favorDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });
        // 购物车表字段： {id , user_id , sku_id , cart_price , sku_num , img_url , sku_name , create_time ,等等}
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cardDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderDS.map(line -> {
            OrderWide orderWide = JSONObject.parseObject(line, OrderWide.class);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(orderWide.getOrder_id());
            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getOrder_price())
                    .orderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });
        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = payDS.map(line -> {
            PaymentWide paymentWide = JSONObject.parseObject(line, PaymentWide.class);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());
            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getOrder_price())
                    .paidOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            String appraise = jsonObject.getString("appraise");
            Long goodcnt = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) goodcnt = 1L;
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodcnt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });
        //TODO 4.Union 7个流
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(
                productStatsWithFavoriteDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPaymentDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS
        );
        //TODO 5.提取时间戳信息
        SingleOutputStreamOperator<ProductStats> WMDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                            @Override
                            public long extractTimestamp(ProductStats productStats, long l) {
                                return productStats.getTs();
                            }
                        }));

        //TODO 6.分组、开窗、聚合   按照sku_id分组，10秒滚动窗口。结合增量聚合（累加值）和全量聚合（提取窗口信息）
        SingleOutputStreamOperator<ProductStats> reduceDS = WMDS.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());

                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
//                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() +
                                stats2.getOrder_sku_num());

                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));
                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
//                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);

                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));
                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
//                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);
                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() +
                                stats2.getGood_comment_ct());
                        return stats1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow timeWindow, Iterable<ProductStats> iterable, Collector<ProductStats> collector) throws Exception {
                        ProductStats stats = iterable.iterator().next();

                        stats.setStt(DateTimeUtil.toYMDhms(new Date(timeWindow.getStart())));
                        stats.setEdt(DateTimeUtil.toYMDhms(new Date(timeWindow.getEnd())));

                        stats.setOrder_ct(stats.getOrderIdSet().size() + 0L);
                        stats.setPaid_order_ct(stats.getPaidOrderIdSet().size() + 0L);
                        stats.setRefund_order_ct(stats.getRefundOrderIdSet().size() + 0L);

                        collector.collect(stats);
                    }
                });

        //TODO 7.关联维度信息

        //TODO 7.1 关联SKU维度信息
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                        productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfo.getLong("TM_ID"));
                        productStats.setCategory3_id(dimInfo.getLong("CATEGOTRY3_ID"));
                    }
                },60, TimeUnit.SECONDS
        );
        //TODO 7.2 关联SPU维度信息

        //TODO 7.3 关联TM维度信息

        //TODO 7.4 关联Category维度信息

        //6.2 补充 SPU 维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws
                                    ParseException {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);
//6.3 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream =
                AsyncDataStream.unorderedWait(productStatsWithSpuDstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws
                                    ParseException {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);
//6.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream =
                AsyncDataStream.unorderedWait(productStatsWithCategory3Dstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws
                                    ParseException {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        productStatsWithTmDstream.print("to save");


        //TODO 8.数据写入ClickHouse
        productStatsWithTmDstream.addSink(
                ClickHouseUtil.getSink("insert into table " +
                        "product_stats valuse(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));
        //TODO 9.启动任务

        //

    }
}
