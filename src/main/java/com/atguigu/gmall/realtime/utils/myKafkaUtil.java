package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class myKafkaUtil {

    private static String brokeList = "hadoop1:9092,hadoop2:9092,hadoop3:9092";
    private static String default_topic = "DWD_DEFALUT_TOPIC";
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
        return new FlinkKafkaProducer(
                brokeList,
                topic,
                new SimpleStringSchema()
        );
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String groupID){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokeList);

        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                properties
        );
    }
    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokeList);
        return new FlinkKafkaProducer<T>(default_topic,
                kafkaSerializationSchema
                ,properties,
                FlinkKafkaProducer.Semantic.NONE);
    }

    public static String getKafkaDDL(String sourceTopic, String groupId) {
        String ddl = "  'connector' = 'kafka',  " +
                "  'topic' = '" + sourceTopic + "',  " +
                "  'properties.bootstrap.servers' = '" + brokeList + "',  " +
                "  'properties.group.id' = '" + groupId + "',  " +
                "  'scan.startup.mode' = 'latest-offset',  " +
                "  'format' = 'json'  ";
        return ddl;
    }
}
