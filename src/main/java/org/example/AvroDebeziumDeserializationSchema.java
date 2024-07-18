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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class AvroDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        LOG.debug(">>> [AVRO-DESERIALIZER] RECORD RECEIVED");
        LOG.debug(String.valueOf(sourceRecord));

        Struct value = (Struct) sourceRecord.value();

        // DDL processing
        boolean isDDL = sourceRecord.valueSchema().field("historyRecord") != null;

        if (isDDL) {
            LOG.debug(">>> [AVRO-DESERIALIZER] DESERIALIZING DDL");

            String historyRecordString = (String) value.get("historyRecord");
            JSONObject historyRecord = JSONObject.parseObject(historyRecordString);
            JSONObject historyRecordPosition = historyRecord.getJSONObject("position");

            JSONObject ddlObject = new JSONObject();

            String databaseName = ((Struct) sourceRecord.key()).getString("databaseName");
            Struct valueSource = value.getStruct("source");

            DateTimeFormatter dateFormatter = DateTimeFormatter
                .ofPattern("yyyy-MM-dd")
                .withZone(ZoneId.systemDefault());

            String binlogFile = historyRecordPosition.getString("file");
            String binlogPos = historyRecordPosition.getString("pos");
            String ddlStatement = historyRecord.getString("ddl");

            if (databaseName.isBlank()) {
                String msg = String.format("INVALID DDL FOUND, MANUAL INTERVENTION NEEDED, STOPPING AT: (%s, %s)", binlogFile, binlogPos);
                LOG.error(">>> [AVRO-DESERIALIZER] {}", msg);
                LOG.error(
                    ">>> POSSIBLE REASON: Table has been changed multiple times, " +
                        "altering a field that does not exist before."
                );
                LOG.error(">>> DDL: {}", ddlStatement);
                throw new RuntimeException(msg);
            }

            String sanitizedDatabaseName = databaseName.replace('-', '_');
            String tableName = valueSource.getString("table");
            String sanitizedTableName = tableName.replace('-', '_');

            ddlObject.put("_db", sanitizedDatabaseName);
            ddlObject.put("_tbl", String.format("_%s_ddl", sanitizedDatabaseName));
            ddlObject.put("_ddl", ddlStatement);
            ddlObject.put("_ddl_tbl", sanitizedTableName);
            ddlObject.put("_binlog_file", binlogFile);
            ddlObject.put("_binlog_pos_end", binlogPos);
            ddlObject.put("_ts", valueSource.getInt64("ts_ms"));

            collector.collect(JSON.toJSONString(ddlObject, SerializerFeature.WriteMapNullValue));

            return;
        }

        LOG.debug(">>> [AVRO-DESERIALIZER] DESERIALIZING ROW");

        String topic = sourceRecord.topic();
        String[] topicSplits = topic.split("\\.");
        String databaseName = topicSplits[1];
        String tableName = topicSplits[2];
        String sanitizedDatabaseName = databaseName.replace('-', '_');
        String sanitizedTableName = tableName.replace('-', '_');

        Struct after = value.getStruct("after");
        JSONObject recordObject = new JSONObject();

        for (Field field : after.schema().fields()) {
            Object o = after.get(field);

            JSONObject valueObject = null;
            if (o != null) {
                String type;
                switch (o.getClass().getSimpleName()) {
                    case "Integer":
                    case "Short":
                        type = "int";
                        break;
                    case "Long":
                        type = "long";
                        break;
                    case "Float":
                        type = "float";
                        break;
                    case "Double":
                        type = "double";
                        break;
                    case "Boolean":
                        type = "boolean";
                        break;
                    default:
                        type = "string";
                        break;
                }

                valueObject = new JSONObject();
                valueObject.put(type, o);
            }

            recordObject.put(field.name().replace('-', '_'), valueObject);
        }

        Struct valueSource = value.getStruct("source");
        Envelope.Operation op = Envelope.operationFor(sourceRecord);

        // DATA

        recordObject.put("_db", sanitizedDatabaseName);
        recordObject.put("_tbl", sanitizedTableName);
        recordObject.put("_op", op);
        recordObject.put("_ts", valueSource.getInt64("ts_ms"));

        collector.collect(JSON.toJSONString(recordObject, SerializerFeature.WriteMapNullValue));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
