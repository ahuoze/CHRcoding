package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
//        SourceRecord{sourcePartition={server=mysql_binlog_source},
//            sourceOffset={file=mysql-bin.000002, pos=2019, row=1, snapshot=true}}
//        ConnectRecord{topic='mysql_binlog_source.gmall-flink.base_trademark',
//            kafkaPartition=null, key=Struct{id=1},
//        keySchema=Schema{mysql_binlog_source.gmall_flink.base_trademark.Key:STRUCT},
//        value=Struct{after=Struct{id=1,tm_name=Redmi,logo_url=aaaa},
//        source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,
//            ts_ms=0,snapshot=true,db=gmall-flink,table=base_trademark
//            ,server_id=0,file=mysql-bin.000002,pos=2019,row=0},op=c,ts_ms=1667208624443},
//        valueSchema=Schema{mysql_binlog_source.gmall_flink.base_trademark.Envelope:STRUCT},
//        timestamp=null, headers=ConnectHeaders(headers=)}


        JSONObject jsonObject = new JSONObject();

        String topic = sourceRecord.topic();
//        System.out.println(topic);
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tablename = fields[2];
        jsonObject.put("database",database);
        jsonObject.put("tablename",tablename);

        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJSON = new JSONObject();
        if(before != null){
            Schema schema = before.schema();
            for (Field field : schema.fields()) {
                Object beforevalue = before.get(field);
                beforeJSON.put(field.name(),beforevalue);
            }
        }
        jsonObject.put("before",beforeJSON);

        Struct after = value.getStruct("after");
        JSONObject afterJSON = new JSONObject();
        if(after != null){
            Schema schema = after.schema();
            for (Field field : schema.fields()) {
                Object aftervalue = after.get(field);
                afterJSON.put(field.name(),aftervalue);
            }
        }
        jsonObject.put("after",afterJSON);

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.name().toLowerCase();
        jsonObject.put("type",type.equals("create") ? "insert" : type);

        collector.collect(jsonObject.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}