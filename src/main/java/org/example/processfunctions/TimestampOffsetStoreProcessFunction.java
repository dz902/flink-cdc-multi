package org.example.processfunctions;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TimestampOffsetStoreProcessFunction extends KeyedProcessFunction<Byte, String, String>
implements CheckpointListener {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    private transient ValueState<Long> lastTimestampOffsetState;
    private transient Collector<String> outCollector;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> descriptor =
            new ValueStateDescriptor<>(
                "lastRecordState",
                TypeInformation.of(new TypeHint<Long>() {})
            );
        lastTimestampOffsetState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        this.outCollector = out;

        JSONObject valueObject = JSONObject.parseObject(value);

        String op = valueObject.getString("_op");

        if ("READ".equals(op)) {
            // SNAPSHOTTING HAS NO TIMESTAMP
            return;
        }

        long timestamp = valueObject.getLongValue("_ts");

        if (timestamp > 0) {
            lastTimestampOffsetState.update(timestamp);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Emit the last record when a checkpoint is completed
        Long lastRecord;

        try {
            lastRecord = lastTimestampOffsetState.value();
        } catch (NullPointerException e) {
            LOG.info(">>> [TIMESTAMP-OFFSET-STORE] CHECKPOINTING BEFORE FIRST MESSAGE, DO NOTHING");
            return;
        }

        if (lastRecord == null || lastRecord <= 0) {
            LOG.info(">>> [TIMESTAMP-OFFSET-STORE] NO TIMESTAMP OFFSET YET");
            return;
        }

        LOG.info(">>> [TIMESTAMP-OFFSET-STORE] CHECKPOINT COMPLETE, SENDING TIMESTAMP");
        LOG.debug(">>> {}", lastRecord);

        this.outCollector.collect(lastRecord.toString());

        lastTimestampOffsetState.update(null);
    }
}