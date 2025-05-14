package org.example.processfunctions.oracle;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ScnOffsetStoreProcessFunction extends KeyedProcessFunction<Byte, String, String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private String lastScn;

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        String scn = jsonObject.getString("_scn");

        if (scn != null && !scn.equals(lastScn)) {
            lastScn = scn;
            out.collect(scn);
        }
    }
} 