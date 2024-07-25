package org.example.processfunctions.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.utils.Thrower;

public class StatusStoreProcessFunction extends KeyedProcessFunction<Byte, String, String>
implements CheckpointListener {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    private transient ValueState<String> jobIDState;
    private transient ValueState<Long> lastRecordTimestampState;
    private transient ValueState<Long> recordCountState;
    private transient Collector<String> outCollector;

    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext runtimeCtx = getRuntimeContext();

        jobIDState = runtimeCtx.getState(new ValueStateDescriptor<>(
            "jobIDState",
            TypeInformation.of(new TypeHint<String>() {})
        ));

        lastRecordTimestampState = runtimeCtx.getState(new ValueStateDescriptor<>(
            "lastRecordState",
            TypeInformation.of(new TypeHint<Long>() {})
        ));

        recordCountState = runtimeCtx.getState(new ValueStateDescriptor<>(
            "recordCountState",
            TypeInformation.of(new TypeHint<Long>() {})
        ));
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        this.outCollector = out;

        RuntimeContext runtimeCtx = getRuntimeContext();

        String jobId = runtimeCtx.getJobId().toString();
        jobIDState.update(jobId);

        JSONObject valueObject = JSONObject.parseObject(value);
        long timestamp = valueObject.getLongValue("_ts");
        lastRecordTimestampState.update(timestamp);

        long recordCount = recordCountState.value() == null ? 0L : recordCountState.value();

        recordCountState.update(recordCount+1);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        String jobID = jobIDState.value();
        String jobName = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap().get("jobName");

        if (StringUtils.isNullOrWhitespaceOnly(jobID) || StringUtils.isNullOrWhitespaceOnly(jobName)) {
            Thrower.errAndThrow("STATUS-STORE", "COULD NOT GET JOB ID");
        }

        long recordCount = recordCountState.value();
        long lastEventTimestamp = lastRecordTimestampState.value();

        JSONObject statusObject = new JSONObject();

        statusObject.put("job_id", jobID);
        statusObject.put("job_name", jobName);
        statusObject.put("record_count", recordCount);
        statusObject.put("last_event_timestamp", lastEventTimestamp);
        statusObject.put("last_checkpoint_timestamp", System.currentTimeMillis());

        LOG.debug(">>> [STATUS-STORE] SENDING STATUS");
        LOG.debug(statusObject.toString());

        this.outCollector.collect(statusObject.toString());
    }
}