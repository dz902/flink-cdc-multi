package org.example;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import com.alibaba.fastjson.JSONObject;
import io.debezium.data.Envelope;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class AvroDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        System.out.println(">>> DESERIALIZE RECORD");
        System.out.println(sourceRecord);

        Struct value = (Struct) sourceRecord.value();

        // DDL processing
        boolean isDDL = sourceRecord.valueSchema().field("historyRecord") != null;

        if (isDDL) {
            System.out.println(">>> DESERIALIZE DDL");

            String historyRecordString = (String) value.get("historyRecord");
            JSONObject historyRecord = JSONObject.parseObject(historyRecordString);
            JSONObject historyRecordPosition = historyRecord.getJSONObject("position");

            JSONObject ddlObject = new JSONObject();

            String database = ((Struct) sourceRecord.key()).getString("databaseName");
            Struct valueSource = value.getStruct("source");

            DateTimeFormatter dateFormatter = DateTimeFormatter
                .ofPattern("yyyy-MM-dd")
                .withZone(ZoneId.systemDefault());

            ddlObject.put("_db", database);
            ddlObject.put("_tbl", database + "_ddl");
            ddlObject.put("_ddl", historyRecord.getString("ddl"));
            ddlObject.put("_ddl_db", valueSource.getString("db"));
            ddlObject.put("_ddl_tbl", valueSource.getString("table"));
            ddlObject.put("_binlog_file", historyRecordPosition.getString("file"));
            ddlObject.put("_binlog_pos_end", historyRecordPosition.getIntValue("pos"));
            ddlObject.put("_ts", valueSource.getInt64("ts_ms"));

            collector.collect(JSON.toJSONString(ddlObject, SerializerFeature.WriteMapNullValue));

            System.out.println("<<< DESERIALIZE DDL");
            return;
        }

        System.out.println(">>> DESERIALIZE RECORD");

        String topic = sourceRecord.topic();
        String[] topicSplits = topic.split("\\.");
        String database = topicSplits[1];
        String table = topicSplits[2];

        Struct after = value.getStruct("after");
        JSONObject recordObject = new JSONObject();

        for (Field field : after.schema().fields()) {
            Object o = after.get(field);

            JSONObject valueObject = null;
            if (o != null) {
                String type = switch (o.getClass().getSimpleName()) {
                    case "Integer", "Short" -> "int";
                    case "Long" -> "long";
                    case "Float" -> "float";
                    case "Double" -> "double";
                    case "Boolean" -> "boolean";
                    default -> "string";
                };

                valueObject = new JSONObject();
                valueObject.put(type, o);
            }

            recordObject.put(field.name(), valueObject);
        }

        Struct valueSource = value.getStruct("source");
        Envelope.Operation op = Envelope.operationFor(sourceRecord);

        // DATA

        recordObject.put("_db", database);
        recordObject.put("_tbl", table);
        recordObject.put("_op", op);
        recordObject.put("_ts", valueSource.getInt64("ts_ms"));

        collector.collect(JSON.toJSONString(recordObject, SerializerFeature.WriteMapNullValue));
        System.out.println("<<< DESERIALIZE RECORD");
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
