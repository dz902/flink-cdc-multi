package org.example.processfunctions.mongodb;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.utils.Thrower;

import java.util.Map;

public class SideInputProcessFunction extends KeyedProcessFunction<Byte, String, String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private final Map<String, Tuple2<OutputTag<String>, String>> tagSchemaStringMap;

    public SideInputProcessFunction(Map<String, Tuple2<OutputTag<String>, String>> tagSchemaStringMap) {
        this.tagSchemaStringMap = tagSchemaStringMap;
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        LOG.debug(">>> [SIDE-INPUT-FUNC] CHECKING STOP SIGNAL");

        if (value.equals("SIGNAL-STOP")) {
            String msg = "STOP SIGNAL RECEIVED, MANUAL INTERVENTION IS NEEDED";
            LOG.error(">>> [SIDE-INPUT-FUNC] {}", msg);
            throw new RuntimeException(msg);
        }

        out.collect(value);

        JSONObject valueJSONObject = JSONObject.parseObject(value);
        String sanitizedCollectionName = valueJSONObject.getString("_coll");
        valueJSONObject.remove("_coll");
        valueJSONObject.remove("_db");

        String filteredValueString = JSON.toJSONString(valueJSONObject, SerializerFeature.WriteMapNullValue);

        Tuple2<OutputTag<String>, String> tagSchemaTuple = tagSchemaStringMap.get(sanitizedCollectionName);
        if (tagSchemaTuple != null) {
            LOG.debug(">>> [SIDE-INPUT-FUNC] SIDE OUTPUT TO: {}", tagSchemaTuple.f0);
            LOG.trace(filteredValueString);
            ctx.output(tagSchemaTuple.f0, filteredValueString);
        } else {
            Thrower.errAndThrow("SIDE-INPUT-FUNC", String.format("UNKNOWN COLLECTION: %s", sanitizedCollectionName));
        }
    }
}
