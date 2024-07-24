package org.example.deserializers;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.utils.AVROUtils;
import org.example.utils.JSONUtils;
import org.example.utils.Sanitizer;
import org.example.utils.Thrower;

import java.util.Map;

public class MongoDebeziumToJSONDeserializer implements DebeziumDeserializationSchema<String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    /*
    * top-level-type = infer top-level type from 100 samples
    * top-level-string = use top-level fields as fields, convert all values into JSON strings
    * doc-string = there is only one field "doc", the whole document is converted into a JSON string
    * */
    private String mode = "top-level-string";
    private final Map<String, Tuple2<OutputTag<String>, Schema>> tagSchemaMap;

    public MongoDebeziumToJSONDeserializer(String mode, Map<String, Tuple2<OutputTag<String>, Schema>> tagSchemaMap) {
        this.mode = mode;
        this.tagSchemaMap = tagSchemaMap;
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        LOG.debug(">>> [AVRO-DESERIALIZER] RECORD RECEIVED");
        LOG.debug(String.valueOf(sourceRecord));

        Struct key = (Struct) sourceRecord.key();
        Struct value = (Struct) sourceRecord.value();
        Struct valueNS = value.getStruct("ns");

        LOG.debug(">>> [AVRO-DESERIALIZER] DESERIALIZING ROW");

        String id = JSONUtils.objectToJSONObject(key.get("_id"))
            .getJSONObject("_id")
            .getString("$oid");
        String databaseName = valueNS.getString("db");
        String collectionName = valueNS.getString("coll");
        String sanitizedDatabaseName = databaseName.replace('-', '_');
        String sanitizedCollectionName = collectionName.replace('-', '_');

        String op = value.getString("operationType").toUpperCase();

        JSONObject recordObject;
        if (op.equals("DELETE")) {
            recordObject = new JSONObject();
        } else {
            Object fullDocument = value.get("fullDocument");
            recordObject = JSONObject.parseObject(JSONObject.toJSONString(fullDocument));
        }

        recordObject.put("_id", id);

        JSONObject sanitizedRecordObject = new JSONObject();

        if ("doc-string".equals(mode)) {
            // TODO: NOT SUPPORTED YET
            Thrower.errAndThrow("", "");
        }

        for (String fieldName : recordObject.keySet()) {
            String sanitizedFieldName = fieldName.replace('-', '_');
            Object v = recordObject.get(fieldName);
            JSONObject valueObject = new JSONObject();

            if ("top-level-string".equals(mode)) {
                if (v != null) {
                    valueObject.put("string", JSON.toJSONString(v));
                } else {
                    valueObject.put("string", null);
                }

                sanitizedRecordObject.put(sanitizedFieldName, valueObject);
            } else {
                if (v != null) {
                    if (v.getClass() == JSONObject.class) {
                        valueObject.put(AVROUtils.getAvroSchemaFrom(v.getClass()).getName(), JSON.toJSONString(v));
                    } else {
                        valueObject.put(AVROUtils.getAvroSchemaFrom(v.getClass()).getName(), v);
                    }

                    sanitizedRecordObject.put(sanitizedFieldName, valueObject);
                } else {
                    sanitizedRecordObject.put(sanitizedFieldName, null);
                }
            }
        }

        switch (op) {
            case "INSERT":
                boolean isSnapshotting = Boolean.parseBoolean(
                    value.getStruct("source").getString("snapshot")
                );

                if (isSnapshotting) {
                    op = "READ";
                }

                break;
            case "UPDATE":
            case "DELETE":
                break;
            default:
                Thrower.errAndThrow("MONGO-DESERIALIZER", String.format("UNKNOWN OPERATION: %s", op));
        }

        sanitizedRecordObject.put("_db", sanitizedDatabaseName);
        sanitizedRecordObject.put("_coll", sanitizedCollectionName);
        sanitizedRecordObject.put("_op", op);
        sanitizedRecordObject.put("_ts", value.getInt64("ts_ms"));

        if ("top-level-string".equals(mode)) {
            Schema schema = tagSchemaMap.get(sanitizedCollectionName).f1;

            for (Schema.Field field : schema.getFields()) {
                String sanitizedFieldName = Sanitizer.sanitize(field.name());

                if (!sanitizedRecordObject.containsKey(sanitizedFieldName)) {
                    LOG.warn(">>> [DESERIALIZER] MISSING FIELD FILLED BY NULL: {}", sanitizedFieldName);
                    LOG.warn(">>> [DESERIALIZER] IF SCHEMA IS NOT STABLE, CONSIDER USING DESERIALIZATION MODE: doc-string");
                    sanitizedRecordObject.put(sanitizedFieldName, null);
                }
            }
        }

        collector.collect(JSON.toJSONString(sanitizedRecordObject, SerializerFeature.WriteMapNullValue));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
