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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MySQLStreamer implements Streamer<String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private final String hostname;
    private final String databaseName;
    private final String username;
    private final String password;
    private final String timezone;
    private final int port;
    private final JSONObject tableNameMap;
    private String offsetFile;
    private String startupMode;
    private int offsetPos;
    private final boolean snapshotOnly; // TODO: SNAPSHOT ONLY MODE
    private JSONObject snapshotConditions; // TODO: FOR REFILL DATA FROM A PERIOD
    private Map<String, Tuple2<OutputTag<String>, Schema>> tagSchemaMap;

    public MySQLStreamer(JSONObject configJSON) {
        this.hostname = Validator.ensureNotEmpty("source.hostname", configJSON.getString("source.hostname"));
        this.port = Integer.parseInt(
            Validator.ensureNotEmpty("source.port", configJSON.getString("source.port"))
        );
        this.databaseName = Validator.ensureNotEmpty("source.database.name", configJSON.getString("source.database.name"));
        this.username = Validator.ensureNotEmpty("source.username", configJSON.getString("source.username"));
        this.password = Validator.ensureNotEmpty("source.password", configJSON.getString("source.password"));
        this.timezone = Validator.withDefault(configJSON.getString("source.timezone"), "UTC");
        this.tableNameMap = configJSON.getJSONObject("table.name.map");

        String offsetValue = configJSON.getString("offset.value");

        if (!StringUtils.isNullOrWhitespaceOnly(offsetValue)) {
            String[] offsetSplits = offsetValue.split("\\.");
            this.offsetFile = offsetSplits[0];
            this.offsetPos = Integer.parseInt(offsetSplits[1]);
        }

        this.startupMode = configJSON.getString("startup.mode");

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

        this.snapshotOnly = Boolean.parseBoolean(configJSON.getString("snapshot.only"));

        if (snapshotOnly) {
            LOG.info(">>> [MYSQL-STREAMER] SNAPSHOT ONLY MODE, STARTUP MODE CHANGED: {} -> initial", startupMode);
        }
    }

    @Override
    public MySqlSource<String> getSource() {
        StartupOptions startupOptions;

        LOG.info(">>> [MONGO-STREAMER] STARTUP MODE: {}", startupMode);

        switch (startupMode) {
            case "earliest":
                startupOptions = StartupOptions.earliest();
                break;
            case "latest":
                startupOptions = StartupOptions.latest();
                break;
            case "offset":
                if (StringUtils.isNullOrWhitespaceOnly(offsetFile) || offsetPos < 0) {
                    LOG.info(">>> [MYSQL-STREAMER] NO VALID OFFSET, STARTUP MODE CHANGED: {} -> initial", startupMode);
                    startupOptions = StartupOptions.initial();
                } else {
                    startupOptions = StartupOptions.specificOffset(offsetFile, offsetPos);
                }
                break;
            case "timestamp":
                // TODO
                //startupOptions = StartupOptions.timestamp();
                Thrower.errAndThrow("MYSQL-STREAMER", "NOT SUPPORTED YET");
                return null;
            default:
                startupOptions = StartupOptions.initial();
        }


        // BASIC BEST PRACTICE DEBEZIUM CONFIGURATIONS

        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("bigint.unsigned.handling.mode","long");
        debeziumProperties.setProperty("decimal.handling.mode","string");
        debeziumProperties.setProperty("database.history.skip.unparseable.ddl", "false");

        return MySqlSource.<String>builder()
            .hostname(hostname)
            .port(port)
            .username(username)
            .password(password)
            .databaseList(databaseName)
            .tableList(databaseName + ".*")
            .serverTimeZone(timezone)
            .scanNewlyAddedTableEnabled(true)
            .deserializer(new MySQLDebeziumToJSONDeserializer(tagSchemaMap))
            .startupOptions(startupOptions)
            .includeSchemaChanges(true)
            .debeziumProperties(debeziumProperties)
            .build();
    }

    @Override
    public Map<String, Tuple2<OutputTag<String>, Schema>> createTagSchemaMap() {
        final String sanitizedDatabaseName = Sanitizer.sanitize(databaseName);
        Map<String, Tuple2<OutputTag<String>, Schema>> tagSchemaMap = new HashMap<>();

        try (Connection connection = DriverManager.getConnection(String.format("jdbc:mysql://%s:%d?tinyInt1isBit=false", hostname, port), username, password)) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables(databaseName, null, "%", new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables
                    .getString(3);
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

                LOG.info(
                    ">>> [MAIN] TAG-SCHEMA MAP FOR: {}{}", String.format("%s.%s", sanitizedDatabaseName, sanitizedTableName) ,
                    (
                        !sanitizedTableName.equals(sanitizedMappedTableName) ? ("(" + sanitizedMappedTableName + ")") : ""
                    )
                );
                LOG.info(String.valueOf(avroSchema));
            }
        } catch (SQLException e) {
            Thrower.errAndThrow("MYSQL-STREAM", ">>> [MAIN] UNABLE TO CONNECT TO SOURCE, EXCEPTION:");
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

        this.tagSchemaMap = tagSchemaMap;

        return tagSchemaMap;
    }

    @Override
    public SingleOutputStreamOperator<String> createMainDataStream(DataStream<String> sourceStream) {
        JSONObject snapshotConfig = new JSONObject();
        snapshotConfig.put("snapshot.only", snapshotOnly);

        return sourceStream
            .keyBy(new NullByteKeySelector<>())
            .process(new DelayedStopSignalProcessFunction(snapshotConfig))
            .setParallelism(1)
            .keyBy(new NullByteKeySelector<>())
            .process(new SideInputProcessFunction(tagSchemaMap))
            .setParallelism(1);
    }
}
