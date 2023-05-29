package com.atguigu.gmall.realtime.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.atguigu.gmall.realtime.app.func.CustomerDeserialization;
import com.atguigu.gmall.realtime.utils.myKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop1:8020/gmall-flink"));
//
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop1")
                .port(3306)
                .username("root")
                .password("12345678")
                .databaseList("gmall-flink")
//                .tableList("gmall-flink.base_trademark")
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.latest())
                .serverTimeZone("UTC")
                .build();
        DataStreamSource<String> stringDataStreamSource = env.addSource(sourceFunction);
        stringDataStreamSource.print();

        String sinkTopic = "ods_base_db";
        stringDataStreamSource.addSink(myKafkaUtil.getKafkaProducer(sinkTopic));

        env.execute("FlinkCDC");

    }
}
