package org.example.deserializers;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson2.JSONException;
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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MongoDBDebeziumToJSONDeserializer implements DebeziumDeserializationSchema<String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    /*
    * top-level-type = infer top-level type from 100 samples
    * top-level-string = use top-level fields as fields, convert all values into JSON strings
    * doc-string = there is only one field "doc", the whole document is converted into a JSON string
    * */
    private String mode = "top-level-string";
    private final Map<String, Tuple2<OutputTag<String>, String>> tagSchemaStringMap;

    public MongoDBDebeziumToJSONDeserializer(String mode, Map<String, Tuple2<OutputTag<String>, String>> tagSchemaStringMap) {
        this.mode = mode;
        this.tagSchemaStringMap = tagSchemaStringMap;
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        LOG.debug(">>> [AVRO-DESERIALIZER] RECORD RECEIVED");
        LOG.debug(String.valueOf(sourceRecord));

        Struct key = (Struct) sourceRecord.key();
        Struct value = (Struct) sourceRecord.value();
        Struct valueNS = value.getStruct("ns");

        LOG.debug(">>> [AVRO-DESERIALIZER] DESERIALIZING ROW");

        String id = null;
        JSONObject idObject = JSONUtils
            .objectToJSONObject(key.get("_id"));

        try {
            JSONObject subIDObject = idObject.getJSONObject("_id");

            // ONLY SPECIAL TREATMENT FOR OBJECT ID TYPE
            id = subIDObject.getString("$oid");

            if (id == null) {
                id = idObject.get("_id").toString();
            }
        } catch (JSONException e) {
            id = idObject.get("_id").toString();

            LOG.debug(">>> [AVRO-DESERIALIZER] NON-STANDARD _id FIELD: {}", id);
        }

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

        for (String fieldName : recordObject.keySet()) {
            String sanitizedFieldName = fieldName.replace('-', '_');
            Object v = recordObject.get(fieldName);

            if (sanitizedFieldName.equals("_id")) {
                sanitizedRecordObject.put(sanitizedFieldName, v);
                continue;
            }

            JSONObject valueObject = new JSONObject();

            if ("top-level-string".equals(mode)) {
                if (v != null) {
                    valueObject.put("string", JSONObject.toJSONString(v));
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
            case "REPLACE":
                op = "UPDATE";
                break;
            default:
                Thrower.errAndThrow("MONGO-DESERIALIZER", String.format("UNKNOWN OPERATION: %s", op));
        }

        JSONObject docObject = sanitizedRecordObject;
        if ("doc-string".equals(mode)) {
            sanitizedRecordObject = new JSONObject();
            sanitizedRecordObject.put("_id", docObject.getString("_id"));
            docObject.remove("_id");
            sanitizedRecordObject.put("doc", docObject.toJSONString());
        }

        sanitizedRecordObject.put("_db", sanitizedDatabaseName);
        sanitizedRecordObject.put("_coll", sanitizedCollectionName);
        sanitizedRecordObject.put("_op", op);
        sanitizedRecordObject.put("_ts", value.getInt64("ts_ms"));

        if ("top-level-string".equals(mode)) {
            // TODO: UGLY
            Map<String, Tuple2<OutputTag<String>, Schema>> tagSchemaMap = tagSchemaStringMap.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> Tuple2.of(entry.getValue().f0, new Schema.Parser().parse(entry.getValue().f1))
                ));

            Schema schema = tagSchemaMap.get(sanitizedCollectionName).f1;
            Set<String> schemaFields = new HashSet<>();

            for (Schema.Field field : schema.getFields()) {
                String sanitizedFieldName = Sanitizer.sanitize(field.name());

                if (!sanitizedRecordObject.containsKey(sanitizedFieldName)) {
                    LOG.warn(">>> [DESERIALIZER] MISSING FIELD FILLED BY NULL: {}", sanitizedFieldName);
                    LOG.warn(">>> [DESERIALIZER] IF SCHEMA IS NOT STABLE, CONSIDER USING DESERIALIZATION MODE: doc-string");
                    sanitizedRecordObject.put(sanitizedFieldName, null);
                }

                schemaFields.add(Sanitizer.sanitize(field.name()));
            }

            JSONArray extraKeys = new JSONArray();
            for (String objKey : sanitizedRecordObject.keySet()) {
                // TODO: THIS IS NOT VERY DUMMY-PROOF
                if (objKey.equals("_db") || objKey.equals("_coll")) {
                    continue;
                }

                if (!schemaFields.contains(objKey)) {
                    extraKeys.add(objKey);
                }
            }

            if (!extraKeys.isEmpty()) {
                String extraKeysString = String.join(", ", (String[]) extraKeys.toArray(new String[0]));
                LOG.error(">>> [DESERIALIZER] DATA LOSS CAUSED BY EXTRA FIELDS: {}", extraKeysString);
                Thrower.errAndThrow("DESERIALIZER",
                    String.format(
                        "SCHEMA CHANGED, MUST MAP COLLECTION TO NEW NAME OR USE doc-string MODE: %s.%s -> %s.%s_vYYYY-MM-DD",
                        databaseName, collectionName, databaseName, collectionName
                    )
                );
                return;
            }
        }

        collector.collect(JSON.toJSONString(sanitizedRecordObject, SerializerFeature.WriteMapNullValue));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
