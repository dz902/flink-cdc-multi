package org.example.processfunctions.mysql;

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

public class SideInputProcessFunction extends KeyedProcessFunction<Byte, String, String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private final Map<String, Tuple2<OutputTag<String>, Schema>> tableTagSchemaMap;

    public SideInputProcessFunction(Map<String, Tuple2<OutputTag<String>, Schema>> tableTagSchemaMap) {
        this.tableTagSchemaMap = tableTagSchemaMap;
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
        String sanitizedTableName = valueJSONObject
            .getString("_tbl");
        valueJSONObject.remove("_tbl");
        valueJSONObject.remove("_db");

        // REMOVE BINLOG INFO, THESE ARE ONLY FOR BINLOG OFFSET WRITE BACK

        if (valueJSONObject.get("_ddl") == null) {
            valueJSONObject.remove("_binlog_file");
            valueJSONObject.remove("_binlog_pos_end");
        }

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
