package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.app.func.CustomerDeserialization;
import com.atguigu.gmall.realtime.app.func.DimSinkFunction;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.myKafkaUtil;
import javafx.scene.control.Tab;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;


//数据流：web/app -> nginx -> SprintBoot -> mysql -> flinkAPP -> kafka(ods) -> flinkAPP -> Kafka(dwd)/Phoenix(dim)
//程  序：         mockDb                -> mysql -> flinkCDC -> kafka(zk) -> BaseDBAPP -> kafka/Phoenix(hbase,zk,hdfs)
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.消费kafka ods_base_db 主题的数据
        String sourceTopic = "ods_base_db";
        String groupID = "base_db_app";
        DataStreamSource<String> kafkaDS = env.addSource(myKafkaUtil.getKafkaConsumer(sourceTopic, groupID));

        //TODO 3.将每行数据转换为JSON对象并过滤掉Delete数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject).filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String type = jsonObject.getString("type");
                        return !type.equals("delete");
                    }
                }
        );

        //TODO 4.使用flinkCDC消费配置表并处理成广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop1")
                .port(3306)
                .username("root")
                .password("12345678")
                .databaseList("gmall-realtime")
                .tableList("gmall-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .serverTimeZone("UTC")
                .build();                                                                                               //自动建表字段
        //  sourceTable     type(操作类型)  sinkType(HBase or Kafka)    sinkTable                                       sinkColumns     pk      extend
        //  base_trademark  insert          hbase                       dim_xxx(Phoenix表名) (要在数据来之前，创建表)
        //  order_info      insert          kafka                       dwd_aaa（主题名）
        //  order_info      update          kafka                       dwd_bbb（主题名）
        DataStreamSource<String> tableProcessStrDS = env.addSource(sourceFunction);
        //key与表中主键一致    value保存整行数据
        MapStateDescriptor mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("map-state",String.class,TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);

        //TODO 5.连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectStream = jsonObjDS.connect(broadcastStream);
        //TODO 6.处理数据 广播流数据，主流数据（根据广播流数据进行处理
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag"){};
        SingleOutputStreamOperator<JSONObject> kafka = connectStream.process(new TableProcessFunction(hbaseTag,mapStateDescriptor));
        //TODO 7.提取kafka流数据和HBase流数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);
        kafka.print("Kafka>>>>");
        hbase.print("Hbase>>>>");
        //TODO 8.将kakfa数据写入kafka主题，将HBase数据写入Phoenix表
        // 为什么不用JDBCsink？因为JDBC要使用类似与“upsert into t values( ? , ? , ? , ? )”的形式，代码不能写死几个字段。
        hbase.addSink(new DimSinkFunction());
//        kafka.map(jsonObj -> jsonObj.toJSONString())
//                .addSink(myKafkaUtil.getKafkaProducer()) 不可用，要写的表在数据里面，这里没办法确定
        //TODO 这里的SerializationSchema没有方泛型方法里实现，因为泛型T不好提取Topic信息，在外部直接确定是JSONObject，可以手动提取。
        kafka.addSink(myKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<byte[], byte[]>(jsonObject.getString("sinkTable"),
                jsonObject.getString("after").getBytes());
            }
        }));
        //TODO 9.启动任务
        env.execute("BaseDBApp");
    }
}
