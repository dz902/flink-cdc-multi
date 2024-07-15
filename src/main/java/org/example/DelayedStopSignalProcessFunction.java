package org.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;

public class DelayedStopSignalProcessFunction extends KeyedProcessFunction<Byte, String, String> {
    private transient ValueState<Long> timerState;
    private transient ValueState<Boolean> stopSignalState;
    private final Map<String, Tuple2<OutputTag<String>, Schema>> tableTagSchemaMap;
    private final OutputTag<String> ddlOutputTag;

    public DelayedStopSignalProcessFunction(Map<String, Tuple2<OutputTag<String>, Schema>> tableTagSchemaMap, OutputTag<String> ddlOutputTag) {
        this.tableTagSchemaMap = tableTagSchemaMap;
        this.ddlOutputTag = ddlOutputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> timerStateDescriptor = new ValueStateDescriptor<>("timerState", Long.class);
        timerState = getRuntimeContext().getState(timerStateDescriptor);

        ValueStateDescriptor<Boolean> stopSignalStateDescriptor = new ValueStateDescriptor<>("stopSignalState", Boolean.class);
        stopSignalState = getRuntimeContext().getState(stopSignalStateDescriptor);
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        if (Boolean.TRUE.equals(stopSignalState.value())) {
            // Ignore messages if stop signal is set
            return;
        }

        out.collect(value);

        JSONObject valueJSONObject = JSONObject.parseObject(value);
        String tableName = valueJSONObject.getString("_tbl");

        Tuple2<OutputTag<String>, Schema> tagSchemaTuple = tableTagSchemaMap.get(tableName);
        if (tagSchemaTuple != null) {
            if (tagSchemaTuple.f0 == ddlOutputTag) {
                System.out.println(">>> DDL FOUND");
                System.out.println(value);

                // Register a timer to trigger after 10 seconds
                long currentTime = ctx.timerService().currentProcessingTime();
                long timerTime = currentTime + 10000; // 10 seconds delay
                ctx.timerService().registerProcessingTimeTimer(timerTime);
                timerState.update(timerTime);
                stopSignalState.update(true);
            }
        } else {
            throw new RuntimeException("Unknown table in message: " + valueJSONObject.toJSONString());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        if (Boolean.TRUE.equals(stopSignalState.value())) {
            System.out.println(">>> SENDING STOP SIGNAL");
            // Send stop signal after 10 seconds
            out.collect("SIGNAL-STOP");
            stopSignalState.clear();
        }
    }
}