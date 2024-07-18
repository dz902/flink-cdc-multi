package org.example;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.avro.Schema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class StopSignalCheckerProcessFunction extends KeyedProcessFunction<Byte, String, String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private final Map<String, Tuple2<OutputTag<String>, Schema>> tableTagSchemaMap;

    public StopSignalCheckerProcessFunction(Map<String, Tuple2<OutputTag<String>, Schema>> tableTagSchemaMap) {
        this.tableTagSchemaMap = tableTagSchemaMap;
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        LOG.debug(">>> [STOP-SIGNAL-CHECKER] CHECKING STOP SIGNAL");

        if (value.equals("SIGNAL-STOP")) {
            String msg = "STOP SIGNAL RECEIVED, DDL FOUND, MANUAL INTERVENTION IS NEEDED";
            LOG.error(">>> [STOP-SIGNAL-CHECKER] {}", msg);
            throw new RuntimeException(msg);
        }

        out.collect(value);

        JSONObject valueJSONObject = JSONObject.parseObject(value);
        String sanitizedTableName = valueJSONObject
            .getString("_tbl");
        valueJSONObject.remove("_tbl");
        valueJSONObject.remove("_db");

        String filteredValue = JSON.toJSONString(valueJSONObject, SerializerFeature.WriteMapNullValue);

        Tuple2<OutputTag<String>, Schema> tagSchemaTuple = tableTagSchemaMap.get(sanitizedTableName);
        if (tagSchemaTuple != null) {
            LOG.debug(">>> [STOP-SIGNAL-CHECKER] SIDE OUTPUT TO: {}", tagSchemaTuple.f0);
            LOG.trace(filteredValue);
            ctx.output(tagSchemaTuple.f0, filteredValue);
        } else {
            LOG.error(">>> [STOP-SIGNAL-CHECKER] UNKNOWN TABLE: {}", sanitizedTableName);
            LOG.error(tableTagSchemaMap.toString());
            throw new RuntimeException();
        }
    }
}
