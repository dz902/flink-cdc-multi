package org.example.deserializers;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class OracleDebeziumToJSONDeserializer implements DebeziumDeserializationSchema<String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        LOG.debug(">>> [ORACLE-DESERIALIZER] Processing record: {}", record);
        Struct value = (Struct) record.value();
        Struct source = value.getStruct("source");
        String operation = value.getString("op");
        Long ts = value.getInt64("ts_ms");
        // Use current time if timestamp is 0 or not set
        ts = ts < 1 ? System.currentTimeMillis() : ts;
        String scn = source.getString("scn");

        // Get database, schema, and table from source struct
        String database = source.getString("db");
        String schema = source.getString("schema");
        String table = source.getString("table");
        LOG.debug(">>> [ORACLE-DESERIALIZER] Database: {}, Schema: {}, Table: {}", database, schema, table);

        JSONObject jsonObject = new JSONObject();

        if (operation.equals("r") || operation.equals("c")) {
            Struct after = value.getStruct("after");
            if (after != null) {
                jsonObject.putAll(structToJson(after));
            }
        } else if (operation.equals("u")) {
            Struct before = value.getStruct("before");
            Struct after = value.getStruct("after");
            if (before != null) {
                jsonObject.putAll(structToJson(before));
            }
            if (after != null) {
                jsonObject.putAll(structToJson(after));
            }
        } else if (operation.equals("d")) {
            Struct before = value.getStruct("before");
            if (before != null) {
                jsonObject.putAll(structToJson(before));
            }
        }

        jsonObject.put("_op", operation);
        jsonObject.put("_ts", ts);
        jsonObject.put("_scn", scn);
        jsonObject.put("_db", database);
        jsonObject.put("_schema", schema);
        jsonObject.put("_tbl", table);

        String jsonString = jsonObject.toJSONString();
        out.collect(jsonString);
    }

    private JSONObject structToJson(Struct struct) {
        JSONObject jsonObject = new JSONObject();
        Schema schema = struct.schema();
        List<Field> fields = schema.fields();

        for (Field field : fields) {
            String fieldName = field.name();
            Object value = struct.get(field);

            if (value instanceof Struct) {
                jsonObject.put(fieldName, structToJson((Struct) value));
            } else if (value instanceof List) {
                List<?> list = (List<?>) value;
                if (!list.isEmpty() && list.get(0) instanceof Struct) {
                    List<JSONObject> jsonList = list.stream()
                        .map(item -> structToJson((Struct) item))
                        .collect(Collectors.toList());
                    jsonObject.put(fieldName, jsonList);
                } else {
                    jsonObject.put(fieldName, list);
                }
            } else if (value instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) value;
                JSONObject mapJson = new JSONObject();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    Object key = entry.getKey();
                    Object val = entry.getValue();
                    if (val instanceof Struct) {
                        mapJson.put(key.toString(), structToJson((Struct) val));
                    } else {
                        mapJson.put(key.toString(), val);
                    }
                }
                jsonObject.put(fieldName, mapJson);
            } else {
                JSONObject valueObject = new JSONObject();
                String type;
                LOG.info(String.format(">>>>> %s = %s = %s", field, Objects.nonNull(value) ? value.getClass() : "NULL", Objects.nonNull(value) ? "null" : String.valueOf(value)));
                if (value instanceof Integer || value instanceof Short) {
                    type = "string";
                    value = String.valueOf(value);
                } else if (value instanceof Long) {
                    type = "string";
                    value = String.valueOf(value);
                } else if (value instanceof Float) {
                    type = "float";
                } else if (value instanceof Double) {
                    type = "double";
                } else if (value instanceof Boolean) {
                    type = "boolean";
                } else {
                    type = "string";
                    value = String.valueOf(value);
                }
                valueObject.put(type, value);
                jsonObject.put(fieldName, valueObject);
            }
        }

        return jsonObject;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
} 