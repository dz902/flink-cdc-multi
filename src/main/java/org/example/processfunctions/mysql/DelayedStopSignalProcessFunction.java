package org.example.processfunctions.mysql;

import com.alibaba.fastjson.JSONArray;
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
    private JSONArray tableArray = null;

    private transient ValueState<Long> timerState;
    private transient ValueState<Boolean> stopSignalState;

    public DelayedStopSignalProcessFunction(JSONObject config) {
        snapshotOnly = Validator.withDefault(config.getBoolean("snapshot.only"), false);
        tableArray = Validator.withDefault(config.getJSONArray("source.table.array"), null);

        LOG.debug("[STOP-SIGNAL-SENDER] SNAPSHOT ONLY: {}", snapshotOnly);
        LOG.debug("[STOP-SIGNAL-SENDER] TARGET TABLE LIST: {}", tableArray);
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
            // IGNORE MESSAGE WHEN STOP SIGNAL IS SENT
            return;
        }

        JSONObject valueJSONObject = JSONObject.parseObject(value);

        if (snapshotOnly) {
            String op = valueJSONObject.getString("_op");

            if (!"READ".equals(op)) {
                LOG.info(">>> [STOP-SIGNAL-SENDER] SNAPSHOT COMPLETE, SENDING STOP SIGNAL");
                setTimer(ctx);
                return;
            }
        }

        out.collect(value);

        String ddlStatement = valueJSONObject.getString("_ddl");

        boolean ddlStatementFound = ddlStatement != null;
        if (ddlStatementFound) {
            boolean onlyStreamingSomeTables = tableArray != null;
            if (onlyStreamingSomeTables) {
                String db = valueJSONObject.getString("_db");
                String ddlTable = valueJSONObject.getString("_ddl_tbl");
                String ddlDBTable = db+"."+ddlTable;

                boolean ddlFoundButNotForTargetTables = !tableArray.contains(ddlDBTable);
                if (ddlFoundButNotForTargetTables) {
                    LOG.info(">>> [STOP-SIGNAL-SENDER] DDL FOUND FOR NON-TARGET TABLE IGNORED: {}", ddlDBTable);
                    LOG.debug(ddlStatement);
                    return;
                }

                LOG.info(">>> [STOP-SIGNAL-SENDER] DDL FOUND FOR TARGET TABLE IGNORED: {}", ddlDBTable);
                LOG.debug(ddlStatement);
            }

            boolean truncateTableStatementFound = ddlStatement.matches("(?i).*TRUNCATE\\s+TABLE.*");
            if (truncateTableStatementFound) {
                LOG.info(">>> [STOP-SIGNAL-SENDER] DDL-TRUNCATE-TABLE FOUND, SHOULD USE SNAPSHOT-ONLY MODE");
            }

            boolean nonStructuralStatementFound = ddlStatement.matches("(?i)(" +
                "CREATE\\s+INDEX.*|" +
                "DROP\\s+INDEX.*|" +
                "ALTER\\s+TABLE.*ADD\\s+INDEX.*|" +
                "ALTER\\s+TABLE.*DROP\\s+INDEX.*|" +
                "ALTER\\s+TABLE.*ADD\\s+KEY.*|" +
                "ALTER\\s+TABLE.*DROP\\s+KEY.*|" +
                "ALTER\\s+TABLE.*ADD\\s+CONSTRAINT.*|" +
                "ALTER\\s+TABLE.*DROP\\s+CONSTRAINT.*|" +
                "ANALYZE\\s+TABLE.*|" +
                "OPTIMIZE\\s+TABLE.*|" +
                "REPAIR\\s+TABLE.*" +
                ")");

            if (nonStructuralStatementFound) {
                LOG.info(">>> [STOP-SIGNAL-SENDER] NON-STRUCTURAL DDL FOUND IGNORED: {}", ddlStatement);
                LOG.debug(ddlStatement);
                return;
            }

            LOG.info(">>> [STOP-SIGNAL-SENDER] SENDING STOP SIGNAL");
            LOG.info(value);

            setTimer(ctx);
        }
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