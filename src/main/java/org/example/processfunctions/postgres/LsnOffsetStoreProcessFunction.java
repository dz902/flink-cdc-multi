package org.example.processfunctions.postgres;

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

public class LsnOffsetStoreProcessFunction extends KeyedProcessFunction<Byte, String, String>
implements CheckpointListener {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    private transient ValueState<String> lastLsnOffsetState;
    private transient Collector<String> outCollector;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> descriptor =
            new ValueStateDescriptor<>(
                "lastRecordState",
                TypeInformation.of(new TypeHint<String>() {})
            );
        lastLsnOffsetState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        this.outCollector = out;

        JSONObject valueObject = JSONObject.parseObject(value);

        String lsn = valueObject.getString("_lsn");

        if (lsn.isBlank()) {
            String op = valueObject.getString("_op");

            if ("READ".equals(op)) {
                // SNAPSHOTTING HAS NO LSN POSITION
                return;
            }

            LOG.error(">>> [LSN-OFFSET-STORE] LSN EMPTY");
            throw new RuntimeException();
        }

        lastLsnOffsetState.update(
            String.format("%s", lsn)
        );
    }
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Emit the last record when a checkpoint is completed
        String lastRecord;

        try {
            lastRecord = lastLsnOffsetState.value();
        } catch (NullPointerException e) {
            LOG.info(">>> [LSN-OFFSET-STORE] CHECKPOINTING BEFORE FIRST MESSAGE, DO NOTHING");
            return;
        }

        if (lastRecord == null || lastRecord.isBlank()) {
            LOG.info(">>> [LSN-OFFSET-STORE] NO LSN OFFSET YET");
            return;
        }

        LOG.info(">>> [LSN-OFFSET-STORE] CHECKPOINT COMPLETE, SENDING LSN");
        LOG.debug(">>> {}", lastRecord);

        this.outCollector.collect(lastRecord);

        lastLsnOffsetState.update(null);
    }
}