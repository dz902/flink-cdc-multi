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

public class MongoAvroDebeziumDeserializer implements DebeziumDeserializationSchema<String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        LOG.debug(">>> [AVRO-DESERIALIZER] RECORD RECEIVED");
        LOG.debug(String.valueOf(sourceRecord));

        Struct value = (Struct) sourceRecord.value();
        Struct valueSource = value.getStruct("source");

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
