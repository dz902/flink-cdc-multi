package org.example.streamers;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.IncrementalSource;
import com.ververica.cdc.connectors.oracle.source.OracleSourceBuilder;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.deserializers.OracleDebeziumToJSONDeserializer;
import org.example.processfunctions.oracle.DelayedStopSignalProcessFunction;
import org.example.processfunctions.oracle.SideInputProcessFunction;
import org.example.utils.AVROUtils;
import org.example.utils.Sanitizer;
import org.example.utils.Thrower;
import org.example.utils.Validator;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class OracleStreamer implements Streamer<String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private final String hostname;
    private final String databaseName;
    private final String schemaName;
    //private final String pdbName;
    private final int splitSize;
    private final int fetchSize;
    private final int logminerParallelism;
    private String[] tableArray;
    private String tableList;
    private final String username;
    private final String password;
    private final int port;
    private final JSONObject tableNameMap;
    private final JSONObject databaseNameMap;
    private String offsetScn;
    private String startupMode;
    private final boolean snapshotOnly;
    private Map<String, Tuple2<OutputTag<String>, String>> tagSchemaStringMap;
    private final String connectionOptions;

    public OracleStreamer(JSONObject configJSON) {
        this.hostname = Validator.ensureNotEmpty("source.hostname", configJSON.getString("source.hostname"));
        this.port = Integer.parseInt(
            Validator.ensureNotEmpty("source.port", configJSON.getString("source.port"))
        );
        this.databaseName = Validator.ensureNotEmpty("source.database.name", configJSON.getString("source.database.name"));
        this.schemaName = Validator.ensureNotEmpty("source.schema.name", configJSON.getString("source.schema.name"));
        //this.pdbName = Validator.ensureNotEmpty("oracle.pdb.name", configJSON.getString("oracle.pdb.name"));
        this.databaseNameMap = configJSON.getJSONObject("database.name.map");

        String allTables = schemaName + ".*";
        this.tableList = Validator.withDefault(configJSON.getString("source.table.list"), allTables);

        if (!tableList.equals(allTables)) {
            this.tableArray = tableList.split(",");

            tableArray = Arrays.stream(tableArray)
                .map(String::trim)
                .map(tbl -> tbl.contains(".") ? tbl : schemaName + "." + tbl)
                .toArray(String[]::new);

            tableList = String.join(",", tableArray);
        } else {
            this.tableArray = null;
        }

        configJSON.put("source.table.array", tableArray);

        LOG.info("[ORACLE-STREAMER] TABLE LIST: {}", tableList);

        this.username = Validator.ensureNotEmpty("source.username", configJSON.getString("source.username"));
        this.password = Validator.ensureNotEmpty("source.password", configJSON.getString("source.password"));
        this.tableNameMap = configJSON.getJSONObject("table.name.map");
        this.connectionOptions = Validator.withDefault(configJSON.getString("oracle.connection.options"), "");

        String offsetValue = configJSON.getString("offset.value");

        if (!StringUtils.isNullOrWhitespaceOnly(offsetValue)) {
            this.offsetScn = offsetValue;
        }

        this.startupMode = Validator.withDefault(configJSON.getString("startup.mode"), "initial");

        switch (startupMode) {
            case "initial":
            case "earliest":
            case "latest":
            case "offset":
                break;
            default:
                startupMode = "initial";
        }

        LOG.info(">>> [ORACLE-STREAMER] STARTUP MODE: {}", startupMode);

        if (!StringUtils.isNullOrWhitespaceOnly(offsetScn)) {
            if (!startupMode.equals("offset")) {
                LOG.info(">>> [ORACLE-STREAMER] OFFSET FOUND, STARTUP MODE CHANGED: {} -> offset", startupMode);
                startupMode = "offset";
            }
        }

        this.snapshotOnly = Validator.withDefault(configJSON.getBooleanValue("snapshot.only"), false);

        if (snapshotOnly) {
            LOG.info(">>> [ORACLE-STREAMER] SNAPSHOT ONLY MODE, STARTUP MODE CHANGED: {} -> initial", startupMode);
        }

        this.splitSize = Validator.withDefault(configJSON.getIntValue("oracle.split.size"), 4096);
        this.fetchSize = Validator.withDefault(configJSON.getIntValue("oracle.fetch.size"), 1024);
        this.logminerParallelism = Validator.withDefault(configJSON.getIntValue("oracle.logminer.parallelism"), 1);

        // Create tag schema map in constructor so it's available for the deserializer
        this.tagSchemaStringMap = createTagSchemaMap();
    }

    @Override
    public IncrementalSource<String, ?> getSource() {
        StartupOptions startupOptions;

        LOG.info(">>> [ORACLE-STREAMER] STARTUP MODE: {}", startupMode);

        switch (startupMode) {
            case "earliest":
                LOG.warn(">>> [ORACLE-STREAMER] STARTING FROM EARLIEST NOT SUPPORT FOR ORACLE: {} -> initial", startupMode);
                startupOptions = StartupOptions.initial();
                break;
            case "latest":
                startupOptions = StartupOptions.latest();
                break;
            case "offset":
                LOG.warn(">>> [ORACLE-STREAMER] STARTING FROM OFFSET NOT SUPPORT FOR ORACLE: {} -> initial", startupMode);
                startupOptions = StartupOptions.initial();
                break;
            default:
                startupOptions = StartupOptions.initial();
        }

        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("decimal.handling.mode", "string");
        debeziumProperties.setProperty("database.history.skip.unparseable.ddl", "false");
        debeziumProperties.setProperty("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.setProperty("database.history.store.only.captured.tables.ddl", "true");
        debeziumProperties.setProperty("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.setProperty("include.schema.changes", "false");
        debeziumProperties.setProperty("database.history.skip.unparseable.ddl", "true");
        //debeziumProperties.setProperty("database.pdb.name", pdbName);

        return OracleSourceBuilder.OracleIncrementalSource.<String>builder()
            .hostname(hostname)
            .port(port)
            .username(username)
            .password(password)
            .databaseList(databaseName)
            .schemaList(schemaName)
            .tableList(tableList)
            .deserializer(new OracleDebeziumToJSONDeserializer())
            .debeziumProperties(debeziumProperties)
            .startupOptions(startupOptions)
            .fetchSize(fetchSize)
            .splitSize(splitSize)
            .build();
    }

    @Override
    public Map<String, Tuple2<OutputTag<String>, String>> createTagSchemaMap() {
        final String sanitizedDatabaseName = Sanitizer.sanitize(databaseName);
        Map<String, Tuple2<OutputTag<String>, Schema>> tagSchemaMap = new HashMap<>();

        String connectionString = String.format("jdbc:oracle:thin:@%s:%d/%s?%s", hostname, port, sanitizedDatabaseName, connectionOptions);
        LOG.info(String.format(">>> [ORACLE-STREAMER] CONNECTING TO: %s", connectionString));

        try (Connection connection = DriverManager.getConnection(
            connectionString,
            username,
            password
        )) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables(null, schemaName, "%", new String[]{"TABLE"});
            List<String> actualTableNames = Arrays.stream(tableArray).map(t -> t.contains(".") ? t.substring(t.lastIndexOf('.') + 1) : t).collect(Collectors.toList());
            LOG.info(">>> [ORACLE-STREAMER] TABLE ARRAY: {}", Arrays.toString(tableArray));
            LOG.info(">>> [ORACLE-STREAMER] ACTUAL TABLE NAMES: {}", actualTableNames);
            while (tables.next()) {
                String tableName = tables.getString(3);
                LOG.debug(">>> [ORACLE-STREAMER] FOUND TABLE: {}", tableName);
                if (!actualTableNames.contains(tableName)) {
                    LOG.debug(">>> [ORACLE-STREAMER] SKIPPING TABLE: {} (not in target list)", tableName);
                    continue;
                }
                String sanitizedTableName = Sanitizer.sanitize(tableName);
                if (!tableName.equals(sanitizedTableName)) {
                    LOG.warn(">>> [ORACLE-STREAMER] TABLE NAME IS SANITIZED: {} -> {}", tableName, sanitizedTableName);
                }

                String mappedTableName;
                String sanitizedMappedTableName = sanitizedTableName;
                if (tableNameMap != null) {
                    mappedTableName = tableNameMap.getString(tableName);
                    if (mappedTableName != null) {
                        sanitizedMappedTableName = Sanitizer.sanitize(mappedTableName);
                        LOG.info(">>> [ORACLE-STREAMER] TABLE NAME MAPPED: {} -> {}", tableName, mappedTableName);
                    }
                }

                String mappedDatabaseName = databaseName;
                String sanitizedMappedDatabaseName = sanitizedDatabaseName;
                if (databaseNameMap != null) {
                    mappedDatabaseName = databaseNameMap.getString(databaseName);
                    if (mappedDatabaseName != null) {
                        sanitizedMappedDatabaseName = Sanitizer.sanitize(mappedDatabaseName);
                    }
                }

                LOG.info(
                    ">>> [MAIN] TAG-SCHEMA MAP FOR: {}{}",
                    String.format("%s.%s", sanitizedMappedDatabaseName, sanitizedTableName),
                    (!sanitizedTableName.equals(sanitizedMappedTableName) ? ("(" + sanitizedMappedTableName + ")") : "")
                );

                ResultSet columns = metaData.getColumns(databaseName, schemaName, tableName, "%");

                SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(sanitizedTableName).fields();
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    String sanitizedColumnName = Sanitizer.sanitize(columnName);

                    if (!columnName.equals(sanitizedColumnName)) {
                        LOG.warn(
                            ">>> [MAIN] COLUMN NAME SANITIZED: ({}) {} -> {}",
                            sanitizedTableName,
                            columnName,
                            sanitizedColumnName
                        );
                    }

                    String columnType = columns.getString("TYPE_NAME");

                    // NOTE: NULL is always allowed
                    LOG.debug(">>> [MAIN] CONVERTING COLUMN: {}.{}: {}", sanitizedTableName, sanitizedColumnName, columnType);

                    AVROUtils.addOracleFieldToFieldAssembler(fieldAssembler, sanitizedColumnName, columnType, true);
                }

                AVROUtils.addFieldToFieldAssembler(fieldAssembler, "_op", "VARCHAR", false);
                AVROUtils.addFieldToFieldAssembler(fieldAssembler, "_ts", "BIGINT", false);

                Schema avroSchema = fieldAssembler.endRecord();

                final String outputTagID = String.format("%s__%s", sanitizedMappedDatabaseName, sanitizedMappedTableName);
                final OutputTag<String> outputTag = new OutputTag<>(outputTagID) {
                };
                // Use database.table as the key to avoid conflicts across databases
                // Use mapped table name for the key, consistent with MySQL
                String tagKey = String.format("%s.%s", sanitizedMappedDatabaseName, sanitizedMappedTableName);
                LOG.info(">>> [ORACLE-STREAMER] CREATING TAG KEY: {} -> {}", tagKey, outputTagID);
                tagSchemaMap.put(tagKey, Tuple2.of(outputTag, avroSchema));

                LOG.info(String.valueOf(avroSchema));
            }
        } catch (SQLException e) {
            Thrower.errAndThrow(
                "ORACLE-STREAM",
                String.format(">>> [MAIN] UNABLE TO CONNECT TO SOURCE, EXCEPTION: %s", e.getMessage())
            );
        }

        // >>> CAPTURE DDL STATEMENTS TO SPECIAL DDL TABLE

        // Apply database name mapping for DDL table as well
        String mappedDatabaseName = databaseName;
        String sanitizedMappedDatabaseName = sanitizedDatabaseName;
        if (databaseNameMap != null) {
            mappedDatabaseName = databaseNameMap.getString(databaseName);
            if (mappedDatabaseName != null) {
                sanitizedMappedDatabaseName = Sanitizer.sanitize(mappedDatabaseName);
            }
        }

        final String sanitizedDDLTableName = String.format("_%s_ddl", sanitizedMappedDatabaseName);
        SchemaBuilder.FieldAssembler<Schema> ddlFieldAssembler = SchemaBuilder.record(sanitizedDDLTableName).fields();

        AVROUtils.addFieldToFieldAssembler(ddlFieldAssembler, "_ddl", "VARCHAR", false);
        AVROUtils.addFieldToFieldAssembler(ddlFieldAssembler, "_ddl_tbl", "VARCHAR", false);
        AVROUtils.addFieldToFieldAssembler(ddlFieldAssembler, "_ts", "BIGINT", false);

        Schema ddlAvroSchema = ddlFieldAssembler.endRecord();

        final String outputTagID = String.format("%s__%s", sanitizedMappedDatabaseName, sanitizedDDLTableName);
        final OutputTag<String> ddlOutputTag = new OutputTag<>(outputTagID) {
        };

        // Use database.table as the key for DDL table as well
        String ddlTagKey = String.format("%s.%s", sanitizedMappedDatabaseName, sanitizedDDLTableName);
        tagSchemaMap.put(ddlTagKey, Tuple2.of(ddlOutputTag, ddlAvroSchema));

        LOG.info(
            ">>> [MAIN] TAG-SCHEMA MAP FOR: {}", String.format("%s.%s", sanitizedMappedDatabaseName, sanitizedDDLTableName)
        );
        LOG.info(String.valueOf(ddlAvroSchema));

        return tagSchemaMap.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> Tuple2.of(entry.getValue().f0, entry.getValue().f1.toString())
            ));
    }

    @Override
    public SingleOutputStreamOperator<String> createMainDataStream(DataStream<String> sourceStream) {
        JSONObject snapshotConfig = new JSONObject();
        snapshotConfig.put("snapshot.only", snapshotOnly);
        snapshotConfig.put("source.table.array", tableArray);

        // Convert database name map to Map<String, String> for SideInputProcessFunction
        Map<String, String> databaseNameMapForFunction = null;
        if (databaseNameMap != null) {
            databaseNameMapForFunction = new HashMap<>();
            for (String key : databaseNameMap.keySet()) {
                databaseNameMapForFunction.put(key, databaseNameMap.getString(key));
            }
        }

        // Convert table name map to Map<String, String> for SideInputProcessFunction
        Map<String, String> tableNameMapForFunction = null;
        if (tableNameMap != null) {
            tableNameMapForFunction = new HashMap<>();
            for (String key : tableNameMap.keySet()) {
                tableNameMapForFunction.put(key, tableNameMap.getString(key));
            }
        }

        return sourceStream
            .keyBy(new NullByteKeySelector<>())
            .process(new DelayedStopSignalProcessFunction(snapshotConfig))
            .setParallelism(1)
            .keyBy(new NullByteKeySelector<>())
            .process(new SideInputProcessFunction(tagSchemaStringMap, databaseNameMapForFunction, tableNameMapForFunction))
            .setParallelism(1);
    }
}