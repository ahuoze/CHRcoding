package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.func.SplitFunction;
import com.atguigu.gmall.realtime.bean.KeywordStats;
import com.atguigu.gmall.realtime.utils.myKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.使用DDL读取Kafka数据
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic ="dwd_page_log";
        tableEnv.executeSql(
                "create table page_view(" +
                        "`common` MAP<STRING,STRING>," +
                        "`page` MAP<STRING,STRING>," +
                        "`ts` BIGINT," +
                        "`rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000))," +
                        "WATERMARK FOR rt AS rt - INTERVAL '1' SECOND " +
                        ")WITH (" + myKafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId) +
                        ")"
        );
        //TODO 3.过滤数据，要求时last_page_id == "search" 并且 搜索内容 item 不为空
        Table fullwordTable = tableEnv.sqlQuery(
                "select " +
                "page['item'] full_word," +
                "rt " +
                "from " +
                "page_view " +
                "where page['last_page_id'] is not null and page['item'] is not null ");
        //TODO 4.注册UDTF，实现分词处理
        tableEnv.createTemporarySystemFunction("split_words", SplitFunction.class);
        Table wordTable = tableEnv.sqlQuery(
                "select " +
                        "word," +
                        "rt " +
                    "from " +
                        fullwordTable + ", LATERAL TABLE (split_words(full_word))"
        );
        //TODO 5.分组开窗聚合
        Table resultTale = tableEnv.sqlQuery(
                "select " +
                        "DATE_FORMAT(window_start,'yyyy-MM-dd HH:mm:ss') stt," +
                        "DATE_FORMAT(window_end,'yyyy-MM-dd HH:mm:ss') edt," +
                        "'search' source," +
                        "word keyword," +
                        "count (*) ct," +
                        "UNIX_TIMESTAMP()*1000 ts " +
                        "from " +
                        "TABLE (TUMBLE(TABLE " + wordTable + ",DESCRIPTOR (rt),INTERVAL '10' SECOND ))"  +
                        " group by " +
                        " word, window_end, window_start"
        );
        //TODO 6.动态表转换为流
        DataStream<KeywordStats> resultDS = tableEnv.toAppendStream(resultTale, KeywordStats.class);
        tableEnv.fromDataStream(resultDS,$("stt"))
        resultDS.print();

        env.execute("KeywordStatsApp");
    }
}
