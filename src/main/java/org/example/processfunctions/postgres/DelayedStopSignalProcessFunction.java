package org.example.processfunctions.postgres;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.utils.Validator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class DelayedStopSignalProcessFunction extends KeyedProcessFunction<Byte, String, String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    private boolean snapshotOnly = false;
    private JSONArray tableArray = null;

    private transient ValueState<Long> timerState;
    private transient ValueState<Boolean> stopSignalState;
    private transient ValueState<Set<String>> processedTablesState;
    private transient ValueState<Boolean> snapshotCompleteState;

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

        ValueStateDescriptor<Set<String>> processedTablesStateDescriptor = new ValueStateDescriptor<>("processedTablesState", TypeInformation.of(new TypeHint<Set<String>>() {}));
        processedTablesState = getRuntimeContext().getState(processedTablesStateDescriptor);

        ValueStateDescriptor<Boolean> snapshotCompleteStateDescriptor = new ValueStateDescriptor<>("snapshotCompleteState", Boolean.class);
        snapshotCompleteState = getRuntimeContext().getState(snapshotCompleteStateDescriptor);
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
            String table = valueJSONObject.getString("_tbl");
            String schema = valueJSONObject.getString("_schema");
            String tableKey = schema + "." + table;

            // Always forward the record first, regardless of operation type
            LOG.debug(">>> [STOP-SIGNAL-SENDER] FORWARDING RECORD: {}", value);
            out.collect(value);

            // Track processed tables for snapshot completion
            Set<String> processedTables = processedTablesState.value();
            if (processedTables == null) {
                processedTables = new HashSet<>();
            }
            processedTables.add(tableKey);
            processedTablesState.update(processedTables);

            // Check if snapshot is complete (all tables have received non-read operations)
            if (!"READ".equals(op)) {
                LOG.info(">>> [STOP-SIGNAL-SENDER] NON-READ OPERATION DETECTED FOR TABLE: {} (op: {})", tableKey, op);
                
                // Check if we have processed all target tables
                boolean allTablesProcessed = true;
                if (tableArray != null && !tableArray.isEmpty()) {
                    for (int i = 0; i < tableArray.size(); i++) {
                        String targetTable = tableArray.getString(i);
                        if (!processedTables.contains(targetTable)) {
                            allTablesProcessed = false;
                            LOG.debug(">>> [STOP-SIGNAL-SENDER] WAITING FOR TABLE: {}", targetTable);
                            break;
                        }
                    }
                }

                if (allTablesProcessed) {
                    LOG.info(">>> [STOP-SIGNAL-SENDER] ALL TABLES PROCESSED, SNAPSHOT COMPLETE, SENDING STOP SIGNAL");
                    snapshotCompleteState.update(true);
                    setTimer(ctx);
                } else {
                    LOG.info(">>> [STOP-SIGNAL-SENDER] WAITING FOR ALL TABLES TO COMPLETE SNAPSHOT. Processed: {}, Target: {}", 
                            processedTables, tableArray);
                }
            }
            
            return;
        }

        out.collect(value);

        String ddlStatement = valueJSONObject.getString("_ddl");

        boolean ddlStatementFound = ddlStatement != null;
        if (ddlStatementFound) {
            boolean onlyStreamingSomeTables = tableArray != null;
            if (onlyStreamingSomeTables) {
                String schema = valueJSONObject.getString("_schema");
                String ddlTable = valueJSONObject.getString("_ddl_tbl");
                String ddlDBTable = schema+"."+ddlTable;

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