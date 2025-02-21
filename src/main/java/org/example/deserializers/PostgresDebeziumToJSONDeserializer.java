package org.example.deserializers;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.utils.Sanitizer;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class PostgresDebeziumToJSONDeserializer implements DebeziumDeserializationSchema<String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        LOG.debug(">>> [AVRO-DESERIALIZER] RECORD RECEIVED");
        LOG.debug(String.valueOf(sourceRecord));

        Struct value = (Struct) sourceRecord.value();
        Struct valueSource = value.getStruct("source");

        // DDL processing
        boolean isDDL = sourceRecord.valueSchema().field("historyRecord") != null;

        if (isDDL) {
            LOG.debug(">>> [AVRO-DESERIALIZER] DESERIALIZING DDL");

            String historyRecordString = (String) value.get("historyRecord");
            JSONObject historyRecord = JSONObject.parseObject(historyRecordString);
            JSONObject historyRecordPosition = historyRecord.getJSONObject("position");

            JSONObject ddlObject = new JSONObject();

            String schemaName = valueSource.getString("schema");
            String tableName = valueSource.getString("table");

            DateTimeFormatter dateFormatter = DateTimeFormatter
                .ofPattern("yyyy-MM-dd")
                .withZone(ZoneId.systemDefault());

            String lsn = historyRecordPosition.getString("lsn");
            String ddlStatement = historyRecord.getString("ddl");

            if (schemaName.isBlank() || tableName.isBlank()) {
                String msg = String.format("INVALID DDL FOUND, MANUAL INTERVENTION NEEDED, STOPPING AT: (%s)", lsn);
                LOG.error(">>> [AVRO-DESERIALIZER] {}", msg);
                LOG.error(
                    ">>> [AVRO-DESERIALIZER] POSSIBLE REASON: Table has been changed multiple times, " +
                        "altering a field that does not exist before."
                );
                LOG.error(">>> DDL: {}", ddlStatement);
                LOG.error(sourceRecord);
                throw new RuntimeException(msg);
            }

            String sanitizedSchemaName = schemaName.replace('-', '_');
            String sanitizedTableName = tableName.replace('-', '_');

            ddlObject.put("_schema", sanitizedSchemaName);
            ddlObject.put("_tbl", String.format("_%s_ddl", sanitizedSchemaName));
            ddlObject.put("_ddl", ddlStatement);
            ddlObject.put("_ddl_tbl", sanitizedTableName);
            ddlObject.put("_lsn", lsn);
            ddlObject.put("_ts", valueSource.getInt64("ts_ms"));

            collector.collect(JSON.toJSONString(ddlObject, SerializerFeature.WriteMapNullValue));

            return;
        }

        LOG.debug(">>> [AVRO-DESERIALIZER] DESERIALIZING ROW");

        String topic = sourceRecord.topic();
        String[] topicSplits = topic.split("\\.", 3);
        String databaseName = topicSplits[1];
        String tableName = topicSplits[2];
        String sanitizedSchemaName = Sanitizer.sanitize(databaseName);
        String sanitizedTableName = Sanitizer.sanitize(tableName);

        String op = Envelope.operationFor(sourceRecord).toString();
        Struct record;

        if (op.equals("DELETE")) {
            record = value.getStruct("before");
        } else {
            record = value.getStruct("after");
        }

        JSONObject recordObject = new JSONObject();

        for (Field field : record.schema().fields()) {
            Object o = record.get(field);

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

            String sanitizedFieldName = Sanitizer.sanitize(field.name());
            recordObject.put(sanitizedFieldName, valueObject);
        }

        // DATA

        recordObject.put("_schema", sanitizedSchemaName);
        recordObject.put("_tbl", sanitizedTableName);
        recordObject.put("_op", op);

        long ts = valueSource.getInt64("ts_ms");
        ts = ts < 1 ? System.currentTimeMillis() : ts;
        recordObject.put("_ts", ts);

        Map<String, ?> sourceOffset = sourceRecord.sourceOffset();
        String lsn = sourceOffset.get("lsn").toString();

        recordObject.put("_lsn", lsn);

        collector.collect(JSON.toJSONString(recordObject, SerializerFeature.WriteMapNullValue));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
