package org.example.processfunctions.oracle;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.utils.Sanitizer;

import java.util.Map;

public class SideInputProcessFunction extends KeyedProcessFunction<Byte, String, String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private final Map<String, Tuple2<OutputTag<String>, String>> tagSchemaStringMap;
    private final Map<String, String> databaseNameMap;
    private final Map<String, String> tableNameMap;

    public SideInputProcessFunction(Map<String, Tuple2<OutputTag<String>, String>> tagSchemaStringMap) {
        this(tagSchemaStringMap, null, null);
    }

    public SideInputProcessFunction(Map<String, Tuple2<OutputTag<String>, String>> tagSchemaStringMap, Map<String, String> databaseNameMap) {
        this(tagSchemaStringMap, databaseNameMap, null);
    }

    public SideInputProcessFunction(Map<String, Tuple2<OutputTag<String>, String>> tagSchemaStringMap, Map<String, String> databaseNameMap, Map<String, String> tableNameMap) {
        this.tagSchemaStringMap = tagSchemaStringMap;
        this.databaseNameMap = databaseNameMap;
        this.tableNameMap = tableNameMap;
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        LOG.debug(">>> [SIDE-INPUT-FUNC] CHECKING STOP SIGNAL");

        if (value.equals("SIGNAL-STOP")) {
            String msg = "STOP SIGNAL RECEIVED, MANUAL INTERVENTION IS NEEDED";
            LOG.error(">>> [SIDE-INPUT-FUNC] {}", msg);
            throw new RuntimeException(msg);
        }

        LOG.debug(">>> [SIDE-INPUT-FUNC] PROCESSING RECORD: {}", value);
        out.collect(value);

        JSONObject valueJSONObject = JSONObject.parseObject(value);
        String sanitizedTableName = valueJSONObject.getString("_tbl");
        String sanitizedDatabaseName = valueJSONObject.getString("_db");
        String schemaName = valueJSONObject.getString("_schema");
        LOG.debug(">>> [STOP-SIGNAL-CHECKER] CDC STREAM SENT: db={}, schema={}, tbl={}", sanitizedDatabaseName, schemaName, sanitizedTableName);
        valueJSONObject.remove("_tbl");
        valueJSONObject.remove("_db");
        valueJSONObject.remove("_schema");

        // REMOVE SCN INFO, THESE ARE ONLY FOR SCN OFFSET WRITE BACK
        if (valueJSONObject.get("_ddl") == null) {
            valueJSONObject.remove("_scn");
        }

        String filteredValue = JSON.toJSONString(valueJSONObject, SerializerFeature.WriteMapNullValue);

        // Sanitize the table and database names for lookup (transparent to user)
        String sanitizedTableNameForLookup = Sanitizer.sanitize(sanitizedTableName);
        String sanitizedDatabaseNameForLookup = Sanitizer.sanitize(sanitizedDatabaseName);
        
        // Apply database name mapping if available
        if (databaseNameMap != null) {
            LOG.debug(">>> [STOP-SIGNAL-CHECKER] LOOKING UP DATABASE MAPPING FOR: {}", sanitizedDatabaseNameForLookup);
            LOG.debug(">>> [STOP-SIGNAL-CHECKER] DATABASE NAME MAP CONTENTS: {}", databaseNameMap.keySet());
            String mappedDatabaseName = databaseNameMap.get(sanitizedDatabaseNameForLookup);
            LOG.debug(">>> [STOP-SIGNAL-CHECKER] DATABASE MAPPING LOOKUP RESULT: {} -> {}", sanitizedDatabaseNameForLookup, mappedDatabaseName);
            if (mappedDatabaseName != null) {
                sanitizedDatabaseNameForLookup = Sanitizer.sanitize(mappedDatabaseName);
                LOG.debug(">>> [STOP-SIGNAL-CHECKER] DATABASE NAME MAPPED: {} -> {}", sanitizedDatabaseName, sanitizedDatabaseNameForLookup);
            } else {
                LOG.debug(">>> [STOP-SIGNAL-CHECKER] NO DATABASE MAPPING FOUND FOR: {}", sanitizedDatabaseNameForLookup);
            }
        }
        
        // Apply table name mapping if available (using original table name)
        // The table mapping uses the original table names from the CDC stream (before sanitization)
        // Reconstruct original table name by reversing the sanitization
        String originalTableName = sanitizedTableName.replace('_', '-');
        LOG.debug(">>> [STOP-SIGNAL-CHECKER] LOOKING UP TABLE MAPPING FOR KEY: {}", originalTableName);
        LOG.debug(">>> [STOP-SIGNAL-CHECKER] TABLE NAME MAP CONTENTS: {}", tableNameMap != null ? tableNameMap.keySet() : "null");
        if (tableNameMap != null) {
            String mappedTableName = tableNameMap.get(originalTableName);
            LOG.debug(">>> [STOP-SIGNAL-CHECKER] TABLE MAPPING LOOKUP RESULT: {} -> {}", originalTableName, mappedTableName);
            if (mappedTableName != null) {
                sanitizedTableNameForLookup = Sanitizer.sanitize(mappedTableName);
                LOG.debug(">>> [STOP-SIGNAL-CHECKER] TABLE NAME MAPPED: {} -> {}", originalTableName, sanitizedTableNameForLookup);
            } else {
                LOG.debug(">>> [STOP-SIGNAL-CHECKER] NO TABLE MAPPING FOUND FOR KEY: {}", originalTableName);
                // Keep the original sanitized table name if no mapping found
                sanitizedTableNameForLookup = sanitizedTableName;
            }
        } else {
            // No table mapping available, use original sanitized table name
            sanitizedTableNameForLookup = sanitizedTableName;
        }
        
        // Special handling for DDL tables: if this is a DDL table, reconstruct the correct DDL table name
        // using the mapped database name (since DDL table names are created with database name prefix)
        if (sanitizedTableNameForLookup.startsWith("_") && sanitizedTableNameForLookup.endsWith("_ddl")) {
            // This is a DDL table, reconstruct the name using the mapped database name
            sanitizedTableNameForLookup = "_" + sanitizedDatabaseNameForLookup + "_ddl";
            LOG.debug(">>> [STOP-SIGNAL-CHECKER] DDL TABLE NAME RECONSTRUCTED: {} -> {}", sanitizedTableName, sanitizedTableNameForLookup);
        }
        
        // Construct the full database.table key for lookup using sanitized names
        String fullTableKey = sanitizedDatabaseNameForLookup + "." + sanitizedTableNameForLookup;
        LOG.debug(">>> [STOP-SIGNAL-CHECKER] FINAL LOOKUP KEY: {}", fullTableKey);
        LOG.debug(">>> [STOP-SIGNAL-CHECKER] AVAILABLE TABLES: {}", String.join(", ", tagSchemaStringMap.keySet()));
        
        Tuple2<OutputTag<String>, String> tagSchemaTuple = tagSchemaStringMap.get(fullTableKey);
        if (tagSchemaTuple != null) {
            LOG.debug(">>> [STOP-SIGNAL-CHECKER] SIDE OUTPUT TO: {}", tagSchemaTuple.f0);
            LOG.debug(">>> [STOP-SIGNAL-CHECKER] SUCCESSFULLY PROCESSED RECORD FOR TABLE: {}", fullTableKey);
            LOG.trace(filteredValue);
            ctx.output(tagSchemaTuple.f0, filteredValue);
        } else {
            LOG.error(">>> [STOP-SIGNAL-CHECKER] UNKNOWN TABLE: {} (original: {}.{})", fullTableKey, sanitizedDatabaseName, sanitizedTableName);
            LOG.error(">>> [STOP-SIGNAL-CHECKER] AVAILABLE TABLES: {}", 
                String.join(", ", tagSchemaStringMap.keySet()));
            LOG.error(">>> [STOP-SIGNAL-CHECKER] RECEIVED DATA: {}", value);
            throw new RuntimeException(">>> [STOP-SIGNAL-CHECKER] UNKNOWN TABLE: " + fullTableKey + " " + String.join(", ", tagSchemaStringMap.keySet()));
        }
    }
} 