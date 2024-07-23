package org.example.processfunctions.mysql;

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

public class BinlogOffsetStoreProcessFunction extends KeyedProcessFunction<Byte, String, String>
implements CheckpointListener {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    private transient ValueState<String> lastBinlogOffsetState;
    private transient Collector<String> outCollector;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<String> descriptor =
            new ValueStateDescriptor<>(
                "lastRecordState",
                TypeInformation.of(new TypeHint<String>() {})
            );
        lastBinlogOffsetState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        this.outCollector = out;

        JSONObject valueObject = JSONObject.parseObject(value);

        String binlogFile = valueObject.getString("_binlog_file");
        String binlogPos = valueObject.getString("_binlog_pos_end");

        if (binlogFile.isBlank() || binlogPos.isBlank()) {
            String op = valueObject.getString("_op");

            if ("READ".equals(op)) {
                // SNAPSHOTTING HAS NO BINLOG POSITION
                return;
            }

            LOG.error(">>> [BINLOG-OFFSET-STORE] BINLOG FILE OR POS EMPTY");
            throw new RuntimeException();
        }

        lastBinlogOffsetState.update(
            String.format("%s,%s", binlogFile, binlogPos)
        );
    }
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Emit the last record when a checkpoint is completed
        String lastRecord;

        try {
            lastRecord = lastBinlogOffsetState.value();
        } catch (NullPointerException e) {
            LOG.info(">>> [BINLOG-OFFSET-STORE] CHECKPOINTING BEFORE FIRST MESSAGE, DO NOTHING");
            return;
        }

        if (lastRecord == null || lastRecord.isBlank()) {
            LOG.info(">>> [BINLOG-OFFSET-STORE] NO BINLOG OFFSET YET");
            return;
        }

        LOG.info(">>> [BINLOG-OFFSET-STORE] CHECKPOINT COMPLETE, SENDING BINLOG");
        LOG.debug(">>> {}", lastRecord);

        this.outCollector.collect(lastRecord);

        lastBinlogOffsetState.update(null);
    }
}