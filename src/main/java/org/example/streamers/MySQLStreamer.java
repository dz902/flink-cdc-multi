package org.example.streamers;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
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
import org.example.deserializers.MySQLDebeziumToJSONDeserializer;
import org.example.processfunctions.mysql.DelayedStopSignalProcessFunction;
import org.example.processfunctions.mysql.SideInputProcessFunction;
import org.example.utils.AVROUtils;
import org.example.utils.Sanitizer;
import org.example.utils.Thrower;
import org.example.utils.Validator;

import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class MySQLStreamer implements Streamer<String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private final String hostname;
    private final String databaseName;
    private final int splitSize;
    private final int fetchSize;
    private String[] tableArray;
    private String tableList;
    private final String username;
    private final String password;
    private final String timezone;
    private final int port;
    private final JSONObject tableNameMap;
    private final JSONObject databaseNameMap;
    private String offsetFile;
    private String startupMode;
    private int offsetPos;
    private final boolean snapshotOnly; // TODO: UPGRADE TO NATIVE v3.1+
    private final String[] snapshotOverridesTableList;
    private final Map<String, String> snapshotOverridesStatements;
    private JSONObject snapshotConditions; // TODO: FOR REFILL DATA FROM A PERIOD
    private Map<String, Tuple2<OutputTag<String>, String>> tagSchemaStringMap;
    private final String serverIdRange;
    private String datetimeOffset; // Add new field for datetime offset

    public MySQLStreamer(JSONObject configJSON) {
        this.hostname = Validator.ensureNotEmpty("source.hostname", configJSON.getString("source.hostname"));
        this.port = Integer.parseInt(
            Validator.ensureNotEmpty("source.port", configJSON.getString("source.port"))
        );
        this.databaseName = Validator.ensureNotEmpty("source.database.name", configJSON.getString("source.database.name"));
        this.databaseNameMap = configJSON.getJSONObject("database.name.map");

        String allTables = databaseName+".*";
        this.tableList = Validator.withDefault(configJSON.getString("source.table.list"), allTables);

        if (!tableList.equals(allTables)) {
            this.tableArray = tableList.split(",");

            tableArray = Arrays.stream(tableArray)
                .map(String::trim)
                .map(tbl -> tbl.contains(".") ? tbl : databaseName +"."+tbl)
                .toArray(String[]::new);

            tableList = String.join(",", tableArray);
        } else {
            this.tableArray = null;
        }

        configJSON.put("source.table.array", tableArray);

        LOG.info("[MYSQL-STREAMER] TABLE LIST: {}", tableList);

        this.username = Validator.ensureNotEmpty("source.username", configJSON.getString("source.username"));
        this.password = Validator.ensureNotEmpty("source.password", configJSON.getString("source.password"));
        this.timezone = Validator.withDefault(configJSON.getString("source.timezone"), "UTC");
        this.tableNameMap = configJSON.getJSONObject("table.name.map");

        String snapshotOverridesTablesString = configJSON.getString("snapshot.select.statement.overrides");

        if (!StringUtils.isNullOrWhitespaceOnly(snapshotOverridesTablesString)) {
            LOG.info("[MYSQL-STREAMER] SNAPSHOT OVERRIDES: {}", snapshotOverridesTablesString);
            this.snapshotOverridesTableList = snapshotOverridesTablesString.split(",");
            this.snapshotOverridesStatements = new HashMap<String, String>();

            for (String tableName : this.snapshotOverridesTableList) {
                LOG.info("[MYSQL-STREAMER] FIND OVERRIDE STATEMENT FOR: {}", tableName);

                tableName = tableName.trim();
                String statementConfigKey = "snapshot.select.statement.overrides." + tableName;
                String statement = Validator.ensureNotEmpty(
                    statementConfigKey,
                    configJSON.getString(statementConfigKey)
                );

                snapshotOverridesStatements.put(tableName, statement);
            }
        } else {
            this.snapshotOverridesTableList = null;
            this.snapshotOverridesStatements = null;
        }

        String offsetValue = configJSON.getString("offset.value");

        if (!StringUtils.isNullOrWhitespaceOnly(offsetValue)) {
            String[] offsetSplits = offsetValue.split(",");
            this.offsetFile = offsetSplits[0];
            this.offsetPos = Integer.parseInt(offsetSplits[1]);
        }

        this.startupMode = Validator.withDefault(configJSON.getString("startup.mode"), "initial");
        this.datetimeOffset = configJSON.getString("datetime.offset"); // Get datetime offset from config

        switch (startupMode) {
            case "initial":
            case "earliest":
            case "latest":
            case "offset":
            case "timestamp":
                break;
            default:
                startupMode = "initial";
        }

        LOG.info(">>> [MYSQL-STREAMER] STARTUP MODE: {}", startupMode);

        if (!StringUtils.isNullOrWhitespaceOnly(offsetFile)) {
            if (!startupMode.equals("offset")) {
                LOG.info(">>> [MYSQL-STREAMER] OFFSET FOUND, STARTUP MODE CHANGED: {} -> offset", startupMode);
                startupMode = "offset";
            }
        }

        if (!StringUtils.isNullOrWhitespaceOnly(datetimeOffset)) {
            if (!startupMode.equals("timestamp")) {
                LOG.info(">>> [MYSQL-STREAMER] DATETIME OFFSET FOUND, STARTUP MODE CHANGED: {} -> timestamp", startupMode);
                startupMode = "timestamp";
            }
        }

        this.snapshotOnly = Boolean.parseBoolean(configJSON.getString("snapshot.only"));

        if (snapshotOnly) {
            LOG.info(">>> [MYSQL-STREAMER] SNAPSHOT ONLY MODE, STARTUP MODE CHANGED: {} -> initial", startupMode);
        }

        this.splitSize = Validator.withDefault(configJSON.getIntValue("mysql.split.size"), 4096);
        this.fetchSize = Validator.withDefault(configJSON.getIntValue("mysql.fetch.size"), 1024);

        this.serverIdRange = configJSON.getString("mysql.server.id.range");

        if (StringUtils.isNullOrWhitespaceOnly(serverIdRange)
            && !serverIdRange.matches("[1-9][0-9]*-[1-9][0-9]*")) {
            Thrower.errAndThrow("MYSQL-STREAMER", "INVALID SERVER ID RANGE: " + serverIdRange);
            return;
        } else {
            LOG.info(">>> [MYSQL-STREAMER] USING SERVER ID RANGE: {}", serverIdRange);
        }
    }

    @Override
    public MySqlSource<String> getSource() {
        StartupOptions startupOptions;

        LOG.info(">>> [MYSQL-STREAMER] STARTUP MODE: {}", startupMode);

        switch (startupMode) {
            case "earliest":
                startupOptions = StartupOptions.earliest();
                break;
            case "latest":
                startupOptions = StartupOptions.latest();
                break;
            case "offset":
                if (StringUtils.isNullOrWhitespaceOnly(offsetFile) || offsetPos < 0) {
                    Thrower.errAndThrow(">>> [MYSQL-STREAMER] NO VALID OFFSET, STARTUP MODE CHANGED: {} -> initial", startupMode);
                    startupOptions = StartupOptions.initial();
                } else {
                    startupOptions = StartupOptions.specificOffset(offsetFile, offsetPos);
                }
                break;
            case "timestamp":
                if (StringUtils.isNullOrWhitespaceOnly(datetimeOffset)) {
                    Thrower.errAndThrow(">>> [MYSQL-STREAMER] NO VALID DATETIME OFFSET, STARTUP MODE CHANGED: {} -> initial", startupMode);
                    startupOptions = StartupOptions.initial();
                } else {
                    try {
                        // Parse the datetime string to timestamp
                        java.time.LocalDateTime dateTime = java.time.LocalDateTime.parse(datetimeOffset);
                        java.time.Instant instant = dateTime.atZone(java.time.ZoneId.of(timezone)).toInstant();
                        long timestamp = instant.toEpochMilli();
                        startupOptions = StartupOptions.timestamp(timestamp);
                        LOG.info(">>> [MYSQL-STREAMER] STARTING FROM TIMESTAMP: {} ({})", datetimeOffset, timestamp);
                    } catch (Exception e) {
                        Thrower.errAndThrow(">>> [MYSQL-STREAMER] INVALID DATETIME FORMAT: {} (expected format: yyyy-MM-ddTHH:mm:ss)", datetimeOffset);
                        return null;
                    }
                }
                break;
            default:
                startupOptions = StartupOptions.initial();
        }


        // BASIC BEST PRACTICE DEBEZIUM CONFIGURATIONS

        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("bigint.unsigned.handling.mode","long");
        debeziumProperties.setProperty("decimal.handling.mode","string");
        debeziumProperties.setProperty("database.history.skip.unparseable.ddl", "false");
        debeziumProperties.setProperty("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.setProperty("database.history.store.only.monitored.tables.ddl", "true");

        // CUSTOM SQL

        if (snapshotOverridesTableList != null) {
            debeziumProperties.setProperty(
                "scan.incremental.snapshot.enabled",
                "false"
            );

            String tableString = String.join(",", snapshotOverridesTableList);
            debeziumProperties.setProperty(
                "snapshot.select.statement.overrides",
                tableString
            );
            LOG.info("[MYSQL-STREAMER] SET SNAPSHOT OVERRIDES TABLES: {}", tableString);

            for (Map.Entry<String, String> entry : snapshotOverridesStatements.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                debeziumProperties.setProperty(
                    "snapshot.select.statement.overrides."+key,
                    value
                );
                LOG.info("[MYSQL-STREAMER] SET OVERRIDE STATEMENT FOR TABLE: {}", key);
            }
        }

        return MySqlSource.<String>builder()
            .hostname(hostname)
            .port(port)
            .username(username)
            .serverId(serverIdRange)
            .password(password)
            .databaseList(databaseName)
            .tableList(tableList)
            .serverTimeZone(timezone)
            .scanNewlyAddedTableEnabled(true)
            .deserializer(new MySQLDebeziumToJSONDeserializer())
            .startupOptions(startupOptions)
            .includeSchemaChanges(true)
            .distributionFactorUpper(10)
            .fetchSize(fetchSize)
            .splitSize(splitSize)
            .debeziumProperties(debeziumProperties)
            .build();
    }

    @Override
    public Map<String, Tuple2<OutputTag<String>, String>> createTagSchemaMap() {
        final String sanitizedDatabaseName = Sanitizer.sanitize(databaseName);
        Map<String, Tuple2<OutputTag<String>, Schema>> tagSchemaMap = new HashMap<>();

        LOG.info(String.format(">>> [MYSQL-STREAMER] CONNECTING TO: %s@%s:%s", username, hostname, port));

        try (Connection connection = DriverManager.getConnection(String.format("jdbc:mysql://%s:%d?tinyInt1isBit=false", hostname, port), username, password)) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables(databaseName, null, "%", new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables.getString(3);
                String sanitizedTableName = Sanitizer.sanitize(tableName);
                if (!tableName.equals(sanitizedTableName)) {
                    LOG.warn(">>> [MYSQL-STREAMER] TABLE NAME IS SANITIZED: {} -> {}", tableName, sanitizedTableName);
                }

                String mappedTableName;
                String sanitizedMappedTableName = sanitizedTableName;
                if (tableNameMap != null) {
                    mappedTableName = tableNameMap.getString(tableName);
                    if (mappedTableName != null) {
                        sanitizedMappedTableName = Sanitizer.sanitize(mappedTableName);
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

                ResultSet columns = metaData.getColumns(databaseName, null, tableName, "%");

                SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(sanitizedTableName).fields();
                while (columns.next()) {
                    String columnName = columns
                        .getString("COLUMN_NAME");
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

                    AVROUtils.addFieldToFieldAssembler(fieldAssembler, sanitizedColumnName, columnType, true);
                }

                AVROUtils.addFieldToFieldAssembler(fieldAssembler, "_op", "VARCHAR", false);
                AVROUtils.addFieldToFieldAssembler(fieldAssembler, "_ts", "BIGINT", false);
                AVROUtils.addFieldToFieldAssembler(fieldAssembler, "_binlog_file_internal", "VARCHAR", false);
                AVROUtils.addFieldToFieldAssembler(fieldAssembler, "_binlog_pos_internal", "BIGINT", false);

                Schema avroSchema = fieldAssembler.endRecord();

                final String outputTagID = String.format("%s__%s", sanitizedMappedDatabaseName, sanitizedTableName);
                final OutputTag<String> outputTag = new OutputTag<>(outputTagID) {};
                tagSchemaMap.put(sanitizedTableName, Tuple2.of(outputTag, avroSchema));

                LOG.info(String.valueOf(avroSchema));
            }
        } catch (SQLException e) {
            Thrower.errAndThrow(
                "MYSQL-STREAM",
                String.format(">>> [MAIN] UNABLE TO CONNECT TO SOURCE, EXCEPTION: %s", e.getMessage())
            );
        }

        // <<<

        // >>> CAPTURE DDL STATEMENTS TO SPECIAL DDL TABLE

        final String sanitizedDDLTableName = String.format("_%s_ddl", sanitizedDatabaseName);
        SchemaBuilder.FieldAssembler<Schema> ddlFieldAssembler = SchemaBuilder.record(sanitizedDDLTableName).fields();

        AVROUtils.addFieldToFieldAssembler(ddlFieldAssembler, "_ddl", "VARCHAR", false);
        AVROUtils.addFieldToFieldAssembler(ddlFieldAssembler, "_ddl_tbl", "VARCHAR", false);
        AVROUtils.addFieldToFieldAssembler(ddlFieldAssembler, "_ts", "BIGINT", false);
        AVROUtils.addFieldToFieldAssembler(ddlFieldAssembler, "_binlog_file", "VARCHAR", false);
        AVROUtils.addFieldToFieldAssembler(ddlFieldAssembler, "_binlog_pos_end", "BIGINT", false);

        Schema ddlAvroSchema = ddlFieldAssembler.endRecord();

        final String outputTagID = String.format("%s__%s", sanitizedDatabaseName, sanitizedDDLTableName);
        final OutputTag<String> ddlOutputTag = new OutputTag<>(outputTagID) {};

        tagSchemaMap.put(sanitizedDDLTableName, Tuple2.of(ddlOutputTag, ddlAvroSchema));

        LOG.info(
            ">>> [MAIN] TAG-SCHEMA MAP FOR: {}", String.format("%s.%s", sanitizedDatabaseName, sanitizedDDLTableName)
        );
        LOG.info(String.valueOf(ddlAvroSchema));

        this.tagSchemaStringMap = tagSchemaMap.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> Tuple2.of(entry.getValue().f0, entry.getValue().f1.toString())
            ));

        return tagSchemaStringMap;
    }

    @Override
    public SingleOutputStreamOperator<String> createMainDataStream(DataStream<String> sourceStream) {
        JSONObject snapshotConfig = new JSONObject();
        snapshotConfig.put("snapshot.only", snapshotOnly);
        snapshotConfig.put("source.table.array", tableArray);

        return sourceStream
            .keyBy(new NullByteKeySelector<>())
            .process(new DelayedStopSignalProcessFunction(snapshotConfig))
            .setParallelism(1)
            .keyBy(new NullByteKeySelector<>())
            .process(new SideInputProcessFunction(tagSchemaStringMap))
            .setParallelism(1);
    }
}
