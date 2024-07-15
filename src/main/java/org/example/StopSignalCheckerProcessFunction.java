package org.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.util.Map;

public class StopSignalCheckerProcessFunction extends KeyedProcessFunction<Byte, String, String> {
    private final Map<String, Tuple2<OutputTag<String>, Schema>> tableTagSchemaMap;

    public StopSignalCheckerProcessFunction(Map<String, Tuple2<OutputTag<String>, Schema>> tableTagSchemaMap) {
        this.tableTagSchemaMap = tableTagSchemaMap;
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        System.out.println(">>> CHECK STOP SIGNAL");

        if (value.equals("SIGNAL-STOP")) {
            System.out.println("DDL found, manual intervention needed.");
            throw new RuntimeException("DDL found, manual intervention needed.");
        }

        out.collect(value);

        JSONObject valueJSONObject = JSONObject.parseObject(value);
        String tableName = valueJSONObject.getString("_tbl");

        Tuple2<OutputTag<String>, Schema> tagSchemaTuple = tableTagSchemaMap.get(tableName);
        if (tagSchemaTuple != null) {
            System.out.println(">>> SIDE OUTPUT TO " + tagSchemaTuple.f0);
            System.out.println(value);
            ctx.output(tagSchemaTuple.f0, value);
        }
    }
}
