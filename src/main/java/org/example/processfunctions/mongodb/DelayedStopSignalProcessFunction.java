package org.example.processfunctions.mongodb;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.utils.Validator;

import java.io.IOException;

public class DelayedStopSignalProcessFunction extends KeyedProcessFunction<Byte, String, String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    private boolean snapshotOnly = false;

    private transient ValueState<Long> timerState;
    private transient ValueState<Boolean> stopSignalState;
    private transient ValueState<Boolean> snapshotCompleteState;

    public DelayedStopSignalProcessFunction(JSONObject config) {
        snapshotOnly = Validator.withDefault(config.getBoolean("snapshot.only"), false);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> timerStateDescriptor = new ValueStateDescriptor<>("timerState", Long.class);
        timerState = getRuntimeContext().getState(timerStateDescriptor);

        ValueStateDescriptor<Boolean> stopSignalStateDescriptor = new ValueStateDescriptor<>("stopSignalState", Boolean.class);
        stopSignalState = getRuntimeContext().getState(stopSignalStateDescriptor);

        ValueStateDescriptor<Boolean> snapshotCompleteStateDescriptor = new ValueStateDescriptor<>("snapshotCompleteState", Boolean.class);
        snapshotCompleteState = getRuntimeContext().getState(snapshotCompleteStateDescriptor);
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        JSONObject valueJSONObject = JSONObject.parseObject(value);

        if (snapshotOnly) {
            String op = valueJSONObject.getString("_op");

            // Check if snapshot is already complete
            if (Boolean.TRUE.equals(snapshotCompleteState.value())) {
                // Snapshot is complete, check if this is a non-read operation
                if (!"READ".equals(op)) {
                    String msg = String.format("SNAPSHOT COMPLETE BUT NON-READ OPERATION DETECTED: op=%s, MANUAL INTERVENTION IS NEEDED", op);
                    LOG.error(">>> [STOP-SIGNAL-SENDER] {}", msg);
                    throw new RuntimeException(msg);
                }
                // If it's a read operation after snapshot complete, ignore it
                return;
            }

            // Always forward the record first, regardless of operation type
            LOG.debug(">>> [STOP-SIGNAL-SENDER] FORWARDING RECORD: {}", value);
            out.collect(value);

            // Check if snapshot is complete (non-read operation detected)
            if (!"READ".equals(op)) {
                LOG.info(">>> [STOP-SIGNAL-SENDER] NON-READ OPERATION DETECTED (op: {}), SNAPSHOT COMPLETE, SENDING STOP SIGNAL", op);
                snapshotCompleteState.update(true);
                setTimer(ctx);
            }
            
            return;
        }

        // Non-snapshot mode: check if stop signal is already sent
        if (Boolean.TRUE.equals(stopSignalState.value())) {
            // IGNORE MESSAGE WHEN STOP SIGNAL IS SENT
            return;
        }

        out.collect(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        if (Boolean.TRUE.equals(stopSignalState.value())) {
            LOG.info(">>> [STOP-SIGNAL-SENDER] SENDING STOP SIGNAL");
            out.collect("SIGNAL-STOP");
            //stopSignalState.clear();
        }
    }

    private void setTimer(Context ctx) throws IOException {
        long currentTime = ctx.timerService().currentProcessingTime();
        long timerTime = currentTime + 10000; // TODO: DELAY UNTIL NEXT CHECKPOINT INTERVAL?
        ctx.timerService().registerProcessingTimeTimer(timerTime);
        timerState.update(timerTime);
        stopSignalState.update(true);
    }
}