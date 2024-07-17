package org.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DelayedStopSignalProcessFunction extends KeyedProcessFunction<Byte, String, String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    private transient ValueState<Long> timerState;
    private transient ValueState<Boolean> stopSignalState;

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
        String ddlStatement = valueJSONObject.getString("_ddl");

        if (ddlStatement != null) {
            LOG.info(">>> [APP/STOP-SIGNAL-SENDER] DDL FOUND, SENDING STOP SIGNAL");
            LOG.info(value);

            // Register a timer to trigger after 10 seconds
            long currentTime = ctx.timerService().currentProcessingTime();
            long timerTime = currentTime + 10000; // 10 seconds delay
            ctx.timerService().registerProcessingTimeTimer(timerTime);
            timerState.update(timerTime);
            stopSignalState.update(true);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        if (Boolean.TRUE.equals(stopSignalState.value())) {
            LOG.info(">>> [APP/STOP-SIGNAL-SENDER] SENDING STOP SIGNAL");
            // Send stop signal after 10 seconds
            out.collect("SIGNAL-STOP");
            stopSignalState.clear();
        }
    }
}