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

public class MySQLDebeziumToJSONDeserializer implements DebeziumDeserializationSchema<String> {
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

            String databaseName = ((Struct) sourceRecord.key()).getString("databaseName");
            String tableName = valueSource.getString("table");

            DateTimeFormatter dateFormatter = DateTimeFormatter
                .ofPattern("yyyy-MM-dd")
                .withZone(ZoneId.systemDefault());

            String binlogFile = historyRecordPosition.getString("file");
            Long binlogPos = historyRecordPosition.getLongValue("pos");
            String ddlStatement = historyRecord.getString("ddl");

            if (databaseName.isBlank() || tableName.isBlank()) {
                String msg = String.format("INVALID DDL FOUND, MANUAL INTERVENTION NEEDED, STOPPING AT: (%s, %s)", binlogFile, binlogPos);
                LOG.error(">>> [AVRO-DESERIALIZER] {}", msg);
                LOG.error(
                    ">>> [AVRO-DESERIALIZER] POSSIBLE REASON: Table has been changed multiple times, " +
                        "altering a field that does not exist before."
                );
                LOG.error(">>> DDL: {}", ddlStatement);
                LOG.error(sourceRecord);
                throw new RuntimeException(msg);
            }

            String sanitizedDatabaseName = databaseName.replace('-', '_');
            String sanitizedTableName = tableName.replace('-', '_');

            ddlObject.put("_db", sanitizedDatabaseName);
            ddlObject.put("_tbl", String.format("_%s_ddl", sanitizedDatabaseName));
            ddlObject.put("_ddl", ddlStatement);
            ddlObject.put("_ddl_tbl", sanitizedTableName);
            ddlObject.put("_binlog_file", binlogFile);
            ddlObject.put("_binlog_pos_end", binlogPos);

            long ts = -1;

            try {
                ts = value.getInt64("ts_ms");
            } catch (Exception e) {
                LOG.error(">>> [AVRO-DESERIALIZER] UNABLE TO DECODE TS_MS: {}", sourceRecord);
                throw e;
            }

            ts = valueSource.getInt64("ts_ms");
            ddlObject.put("_ts", ts);

            collector.collect(JSON.toJSONString(ddlObject, SerializerFeature.WriteMapNullValue));

            return;
        }

        LOG.debug(">>> [AVRO-DESERIALIZER] DESERIALIZING ROW");

        String topic = sourceRecord.topic();
        String[] topicSplits = topic.split("\\.", 3);
        String databaseName = topicSplits[1];
        String tableName = topicSplits[2];
        String sanitizedDatabaseName = Sanitizer.sanitize(databaseName);
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

        recordObject.put("_db", sanitizedDatabaseName);
        recordObject.put("_tbl", sanitizedTableName);
        recordObject.put("_op", op);

        long ts = -1;

        try {
            ts = value.getInt64("ts_ms");
        } catch (Exception e) {
            LOG.error(">>> [AVRO-DESERIALIZER] UNABLE TO DECODE TS_MS: {}", sourceRecord);
            throw e;
        }

        ts = ts < 1 ? System.currentTimeMillis() : ts;
        recordObject.put("_ts", ts);

        Map<String, ?> sourceOffset = sourceRecord.sourceOffset();
        String binlogFile = sourceOffset.get("file").toString();
        String binlogPos = sourceOffset.get("pos").toString();
        String binlogFileInternal = valueSource.getString("file");
        Long binlogPosInternal = valueSource.getInt64("pos");

        // EXTRA DATA, ONLY FOR BINLOG OFFSET WRITE BACK
        // NOTE: WE HAVE TO USE STARTING BINLOG OFFSET UNLIKE DDL
        // BECAUSE ENDING OFFSET COULD LAND US ON MID TRANSACTION AND FAIL

        recordObject.put("_binlog_file", binlogFile);
        recordObject.put("_binlog_pos_end", binlogPos);
        recordObject.put("_binlog_file_internal", binlogFileInternal);
        recordObject.put("_binlog_pos_internal", binlogPosInternal);

        collector.collect(JSON.toJSONString(recordObject, SerializerFeature.WriteMapNullValue));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
