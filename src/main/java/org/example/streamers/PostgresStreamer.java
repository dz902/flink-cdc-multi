package org.example.streamers;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.IncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
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
import org.example.deserializers.PostgresDebeziumToJSONDeserializer;
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

public class PostgresStreamer implements Streamer<String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private final String hostname;
    private final String databaseName;
    private final String schemaName;
    private final int splitSize;
    private final int fetchSize;
    private String[] tableArray;
    private String tableList;
    private final String username;
    private final String password;
    private final int port;
    private final JSONObject tableNameMap;
    private String offsetFile;
    private String startupMode;
    private int offsetPos;
    private final boolean snapshotOnly; // TODO: UPGRADE TO NATIVE v3.1+
    private JSONObject snapshotConditions; // TODO: FOR REFILL DATA FROM A PERIOD
    private Map<String, Tuple2<OutputTag<String>, String>> tagSchemaStringMap;

    public PostgresStreamer(JSONObject configJSON) {
        this.hostname = Validator.ensureNotEmpty("source.hostname", configJSON.getString("source.hostname"));
        this.port = Integer.parseInt(
            Validator.ensureNotEmpty("source.port", configJSON.getString("source.port"))
        );
        this.databaseName = Validator.ensureNotEmpty("source.database.name", configJSON.getString("source.database.name"));
        this.schemaName = Validator.ensureNotEmpty("source.schema.name", configJSON.getString("source.schema.name"));

        String allTables = schemaName+".*";
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

        LOG.info("[POSTGRE-STREAMER] TABLE LIST: {}", tableList);

        this.username = Validator.ensureNotEmpty("source.username", configJSON.getString("source.username"));
        this.password = Validator.ensureNotEmpty("source.password", configJSON.getString("source.password"));
        // this.timezone = Validator.withDefault(configJSON.getString("source.timezone"), "UTC");
        this.tableNameMap = configJSON.getJSONObject("table.name.map");

        String offsetValue = configJSON.getString("offset.value");

        if (!StringUtils.isNullOrWhitespaceOnly(offsetValue)) {
            String[] offsetSplits = offsetValue.split(",");
            this.offsetFile = offsetSplits[0];
            this.offsetPos = Integer.parseInt(offsetSplits[1]);
        }

        this.startupMode = Validator.withDefault(configJSON.getString("startup.mode"), "initial");

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

        LOG.info(">>> [POSTGRES-STREAMER] STARTUP MODE: {}", startupMode);

        if (!StringUtils.isNullOrWhitespaceOnly(offsetFile)) {
            if (!startupMode.equals("offset")) {
                LOG.info(">>> [POSTGRES-STREAMER] OFFSET FOUND, STARTUP MODE CHANGED: {} -> offset", startupMode);
                startupMode = "offset";
            }
        }

        this.snapshotOnly = Boolean.parseBoolean(configJSON.getString("snapshot.only"));

        if (snapshotOnly) {
            LOG.info(">>> [POSTGRES-STREAMER] SNAPSHOT ONLY MODE, STARTUP MODE CHANGED: {} -> initial", startupMode);
        }

        this.splitSize = Validator.withDefault(configJSON.getIntValue("postgre.split.size"), 4096);
        this.fetchSize = Validator.withDefault(configJSON.getIntValue("postgre.fetch.size"), 1024);
    }

    @Override
    public IncrementalSource<String, ?> getSource() {
        StartupOptions startupOptions;

        LOG.info(">>> [POSTGRES-STREAMER] STARTUP MODE: {}", startupMode);

        switch (startupMode) {
            case "earliest":
                startupOptions = StartupOptions.earliest();
                break;
            case "latest":
                startupOptions = StartupOptions.latest();
                break;
            case "offset":
                if (StringUtils.isNullOrWhitespaceOnly(offsetFile) || offsetPos < 0) {
                    Thrower.errAndThrow(">>> [POSTGRES-STREAMER] NO VALID OFFSET, STARTUP MODE CHANGED: {} -> initial", startupMode);
                    startupOptions = StartupOptions.initial();
                } else {
                    startupOptions = StartupOptions.specificOffset(offsetFile, offsetPos);
                }
                break;
            case "timestamp":
                // TODO
                //startupOptions = StartupOptions.timestamp();
                Thrower.errAndThrow("POSTGRES-STREAMER", "NOT SUPPORTED YET");
                return null;
            default:
                startupOptions = StartupOptions.initial();
        }


        // BASIC BEST PRACTICE DEBEZIUM CONFIGURATIONS

        Properties debeziumProperties = new Properties();
//        debeziumProperties.setProperty("bigint.unsigned.handling.mode","long");
//        debeziumProperties.setProperty("decimal.handling.mode","string");
//        debeziumProperties.setProperty("database.history.skip.unparseable.ddl", "false");
//        debeziumProperties.setProperty("database.history.store.only.monitored.tables.ddl", "true");
//        debeziumProperties.setProperty("database.history.store.only.monitored.tables.ddl", "true");

        return PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
            .hostname(hostname)
            .port(port)
            .username(username)
            .password(password)
            .database(databaseName)
            .schemaList(schemaName)
            .tableList(tableList)
            .includeSchemaChanges(true)
            .decodingPluginName("pgoutput")
            .deserializer(new PostgresDebeziumToJSONDeserializer())
            .debeziumProperties(debeziumProperties)
            .distributionFactorUpper(10)
            .startupOptions(startupOptions)
            .fetchSize(fetchSize)
            .splitSize(splitSize)
            .build();
    }

    @Override
    public Map<String, Tuple2<OutputTag<String>, String>> createTagSchemaMap() {
        final String sanitizedDatabaseName = Sanitizer.sanitize(databaseName);
        Map<String, Tuple2<OutputTag<String>, Schema>> tagSchemaMap = new HashMap<>();

        LOG.info(String.format(">>> [POSTGRES-STREAMER] CONNECTING TO: %s@%s:%s", username, hostname, port));

        // TODO: SSL?
        try (Connection connection = DriverManager.getConnection(String.format("jdbc:postgresql://%s:%d/%s?sslmode=disable", hostname, port, databaseName), username, password)) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables(null, schemaName, "%", new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables
                    .getString(3);
                String sanitizedTableName = Sanitizer.sanitize(tableName);
                if (!tableName.equals(sanitizedTableName)) {
                    LOG.warn(">>> [POSTGRES-STREAMER] TABLE NAME IS SANITIZED: {} -> {}", tableName, sanitizedTableName);
                }

                String mappedTableName;
                String sanitizedMappedTableName = sanitizedTableName;
                if (tableNameMap != null) {
                    mappedTableName = tableNameMap.getString(tableName);
                    if (mappedTableName != null) {
                        sanitizedMappedTableName = Sanitizer.sanitize(mappedTableName);
                    }
                }

                LOG.info(
                    ">>> [MAIN] TAG-SCHEMA MAP FOR: {}{}", String.format("%s.%s", sanitizedDatabaseName, sanitizedTableName) ,
                    (
                        !sanitizedTableName.equals(sanitizedMappedTableName) ? ("(" + sanitizedMappedTableName + ")") : ""
                    )
                );

                // TODO: MULTIPLE DB?

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

                Schema avroSchema = fieldAssembler.endRecord();

                final String outputTagID = String.format("%s__%s", sanitizedDatabaseName, sanitizedMappedTableName);
                final OutputTag<String> outputTag = new OutputTag<>(outputTagID) {};
                tagSchemaMap.put(sanitizedTableName, Tuple2.of(outputTag, avroSchema));

                LOG.info(String.valueOf(avroSchema));
            }
        } catch (SQLException e) {
            Thrower.errAndThrow(
                "POSTGRE-STREAM",
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
        AVROUtils.addFieldToFieldAssembler(ddlFieldAssembler, "_lsn", "VARCHAR", false);

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
