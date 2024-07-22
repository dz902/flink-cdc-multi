package org.example.deserializers;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.utils.JSONUtils;

public class MongoAvroDebeziumDeserializer implements DebeziumDeserializationSchema<String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        LOG.debug(">>> [AVRO-DESERIALIZER] RECORD RECEIVED");
        LOG.debug(String.valueOf(sourceRecord));

        Struct value = (Struct) sourceRecord.value();
        Struct valueNS = value.getStruct("ns");

        LOG.debug(">>> [AVRO-DESERIALIZER] DESERIALIZING ROW");

        String id = JSONUtils.objectToJSONObject(value.get("_id"))
            .getJSONObject("_id")
            .getString("$oid");
        String databaseName = valueNS.getString("db");
        String collectionName = valueNS.getString("coll");
        String sanitizedDatabaseName = databaseName.replace('-', '_');
        String sanitizedCollectionName = collectionName.replace('-', '_');

        Object fullDocument = value.get("fullDocument");
        JSONObject recordObject = JSONObject.parseObject(JSONObject.toJSONString(fullDocument));
        recordObject.put("_id", id);

        JSONObject sanitizedRecordObject = new JSONObject();
        for (String fieldName : recordObject.keySet()) {
            String sanitizedFieldName = fieldName.replace('-', '_');
            sanitizedRecordObject.put(sanitizedFieldName, recordObject.get(fieldName));
        }

        Envelope.Operation op = Envelope.operationFor(sourceRecord);

        // DATA

        recordObject.put("_db", sanitizedDatabaseName);
        recordObject.put("_coll", sanitizedCollectionName);
        recordObject.put("_op", op);
        recordObject.put("_ts", value.getInt64("ts_ms"));

        collector.collect(JSON.toJSONString(recordObject, SerializerFeature.WriteMapNullValue));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
