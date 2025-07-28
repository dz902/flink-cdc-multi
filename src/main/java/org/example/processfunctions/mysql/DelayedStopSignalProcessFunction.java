package org.example.processfunctions.mysql;

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
    private transient ValueState<Set<String>> completedTablesState;
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

        ValueStateDescriptor<Set<String>> completedTablesStateDescriptor = new ValueStateDescriptor<>("completedTablesState", TypeInformation.of(new TypeHint<Set<String>>() {}));
        completedTablesState = getRuntimeContext().getState(completedTablesStateDescriptor);

        ValueStateDescriptor<Boolean> snapshotCompleteStateDescriptor = new ValueStateDescriptor<>("snapshotCompleteState", Boolean.class);
        snapshotCompleteState = getRuntimeContext().getState(snapshotCompleteStateDescriptor);
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        JSONObject valueJSONObject = JSONObject.parseObject(value);

        if (snapshotOnly) {
            String op = valueJSONObject.getString("_op");
            String table = valueJSONObject.getString("_tbl");
            String database = valueJSONObject.getString("_db");
            String tableKey = database + "." + table;

            // Check if snapshot is already complete
            if (Boolean.TRUE.equals(snapshotCompleteState.value())) {
                // Snapshot is complete, check if this is a non-read operation
                if (!"READ".equals(op)) {
                    String msg = String.format("SNAPSHOT COMPLETE BUT NON-READ OPERATION DETECTED: table=%s, op=%s, MANUAL INTERVENTION IS NEEDED", tableKey, op);
                    LOG.error(">>> [STOP-SIGNAL-SENDER] {}", msg);
                    throw new RuntimeException(msg);
                }
                // If it's a read operation after snapshot complete, ignore it
                return;
            }

            // Always forward the record first, regardless of operation type
            LOG.debug(">>> [STOP-SIGNAL-SENDER] FORWARDING RECORD: {}", value);
            out.collect(value);

            // Track processed tables (tables that have received any records)
            Set<String> processedTables = processedTablesState.value();
            if (processedTables == null) {
                processedTables = new HashSet<>();
            }
            processedTables.add(tableKey);
            processedTablesState.update(processedTables);

            // Track completed tables (tables that have received non-read operations)
            Set<String> completedTables = completedTablesState.value();
            if (completedTables == null) {
                completedTables = new HashSet<>();
            }

            // Check if this is a non-read operation
            if (!"READ".equals(op)) {
                LOG.info(">>> [STOP-SIGNAL-SENDER] NON-READ OPERATION DETECTED FOR TABLE: {} (op: {})", tableKey, op);
                completedTables.add(tableKey);
                completedTablesState.update(completedTables);
                
                // Check if all target tables have completed their snapshot
                boolean allTablesCompleted = true;
                if (tableArray != null && !tableArray.isEmpty()) {
                    for (int i = 0; i < tableArray.size(); i++) {
                        String targetTable = tableArray.getString(i);
                        if (!completedTables.contains(targetTable)) {
                            allTablesCompleted = false;
                            LOG.debug(">>> [STOP-SIGNAL-SENDER] WAITING FOR TABLE TO COMPLETE SNAPSHOT: {}", targetTable);
                            break;
                        }
                    }
                } else {
                    // If no specific table list, check if all processed tables have completed
                    allTablesCompleted = processedTables.equals(completedTables);
                }

                if (allTablesCompleted) {
                    LOG.info(">>> [STOP-SIGNAL-SENDER] ALL TABLES COMPLETED SNAPSHOT, SENDING STOP SIGNAL");
                    LOG.info(">>> [STOP-SIGNAL-SENDER] Processed tables: {}", processedTables);
                    LOG.info(">>> [STOP-SIGNAL-SENDER] Completed tables: {}", completedTables);
                    snapshotCompleteState.update(true);
                    setTimer(ctx);
                } else {
                    LOG.info(">>> [STOP-SIGNAL-SENDER] WAITING FOR ALL TABLES TO COMPLETE SNAPSHOT. Processed: {}, Completed: {}, Target: {}", 
                            processedTables, completedTables, tableArray);
                }
            } else {
                LOG.debug(">>> [STOP-SIGNAL-SENDER] READ OPERATION FOR TABLE: {} (op: {})", tableKey, op);
            }
            
            return;
        }

        // Non-snapshot mode: check if stop signal is already sent
        if (Boolean.TRUE.equals(stopSignalState.value())) {
            // IGNORE MESSAGE WHEN STOP SIGNAL IS SENT
            return;
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

            boolean nonStructuralStatementFound = ddlStatement.matches("(?i)(?s)(" +
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