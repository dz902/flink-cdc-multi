package org.example;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;

public class DataStreamJob {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    public static void main(String[] args) throws Exception {
        // FLINK ENV SETUP

        LOG.info(">>> [MAIN] SETTING UP FLINK ENV");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Configuration flinkConfig = GlobalConfiguration.loadConfiguration();
        env.configure(flinkConfig);

        // YOU MUST MANUALLY LOAD CONFIG FOR S3 REGION TO TAKE EFFECT

        // FileSystem.initialize(flinkConfig, null);

        // <<<

        // CLI OPTIONS >>>
        LOG.info(">>> [MAIN] ARGS: {}", Arrays.toString(args));

        Options options = new Options();
        options.addOption("c", "config", true, "config json");
        options.addOption(null, "debug", false, "Enable debug logging.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("debug")) {
            LOG.info(">>> [MAIN] DEBUG MODE");

            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();

            LoggerConfig loggerConfig = new LoggerConfig("flink-cdc-multi", Level.TRACE, true);
            config.addLogger("flink-cdc-multi", loggerConfig);

            ctx.updateLoggers();
        }

        final String argConfig = cmd.getOptionValue("c");

        JSONObject tableNameMap = null;
        JSONObject binlogOffset = null;
        String hostname = "localhost";
        int port = 3306;
        String username = "test";
        String password = "";
        String sinkPath = "";
        String sourceId = "";
        String databaseName = "";
        String timezone = "UTC";
        String checkpointStorage;
        String checkpointDirectory;
        int checkpointInterval = 30;
        Configuration checkpointConfig = new Configuration();

        // TODO: WRITE CONFIGURATION BACK
        // TODO: ADD STATS TABLE

        if (argConfig != null) {
            LOG.info(">>> [MAIN] LOADING CONFIG FROM {}", argConfig);

            Path configPath = new Path(argConfig);
            FileSystem configFS = configPath.getFileSystem();
            String configJSONString;

            try (FSDataInputStream configInputStream = configFS.open(configPath)) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(configInputStream));

                StringBuilder content = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    content.append(line).append("\n");
                }

                configJSONString = content.toString();
            }

            JSONObject configJSON;
            try {
                configJSON = JSONObject.parseObject(configJSONString);
            } catch (Exception e) {
                // do not print json contents as it may contain credentials
                LOG.error(">>> [MAIN] CONFIG JSON IS NOT VALID");
                throw new RuntimeException();
            }

            tableNameMap = configJSON.getJSONObject("table.name.map");

            if (tableNameMap != null) {
                LOG.info(">>> [MAIN] LOADED TABLE NAME MAP: {}", tableNameMap);
            }

            binlogOffset = configJSON.getJSONObject("binlog.offset");
            sinkPath = configJSON.getString("sink.path");
            sourceId = configJSON.getString("source.id");
            hostname = Objects.requireNonNullElse(configJSON.getString("source.hostname"), hostname);
            port = configJSON.getIntValue("source.port") > 0 ? configJSON.getIntValue("source.port") : port;
            username = Objects.requireNonNullElse(configJSON.getString("source.username"), username);
            password = Objects.requireNonNullElse(configJSON.getString("source.password"), password);
            databaseName = configJSON.getString("source.database.name");
            timezone = Objects.requireNonNullElse(configJSON.getString("source.timezone"), timezone);
            checkpointInterval = configJSON.getIntValue("checkpoint.interval") > 0 ? configJSON.getIntValue("checkpoint.interval") : checkpointInterval;
            checkpointStorage = Objects.requireNonNullElse(configJSON.getString("checkpoint.storage"), "jobmanager");

            if (checkpointStorage.equals("jobmanager") || checkpointStorage.equals("filesystem")) {
                if (checkpointStorage.equals("filesystem")) {
                    LOG.info(">>> [MAIN] LOADED CHECKPOINT STORAGE: {}", checkpointStorage);

                    checkpointDirectory = configJSON.getString("checkpoint.directory");

                    if (checkpointDirectory == null) {
                        LOG.error(">>> [MAIN] CHECKPOINT DIRECTORY IS EMPTY FOR FILESYSTEM STORAGE");
                        throw new RuntimeException();
                    }

                    checkpointConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, checkpointStorage);
                    checkpointConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDirectory);
                }
            } else {
                LOG.error(">>> [MAIN] CHECKPOINT STORAGE NOT IN FORMAT: filesystem | jobmanager");
                throw new RuntimeException();
            }

            LOG.info(">>> [MAIN] LOADED SINK PATH: {}", sinkPath);
            LOG.info(">>> [MAIN] LOADED SOURCE: {}:{}", hostname, port);
        } else {
            LOG.warn(">>> [MAIN] NO CONFIG PROVIDED");
        }

        String sanitizedDatabaseName = databaseName.replace('-', '_');
        if (!databaseName.equals(sanitizedDatabaseName)) {
            LOG.warn(">>> [MAIN] DATABASE NAME IS SANITIZED: {} -> {}", databaseName, sanitizedDatabaseName);
        }

        env.configure(checkpointConfig);
        env.enableCheckpointing(checkpointInterval * 1000L);

        // BINLOG OFFSET

        StartupOptions startupOptions = StartupOptions.initial();
        if (binlogOffset != null) {
            String binlogOffsetFile = binlogOffset.getString("file");
            long binlogOffsetPos = binlogOffset.getLongValue("pos");

            if (binlogOffsetFile != null && binlogOffsetPos > 0) {
                startupOptions = StartupOptions.specificOffset(binlogOffsetFile, binlogOffsetPos);
            } else {
                LOG.error(">>> [MAIN] BINLOG OFFSET NOT IN FORMAT: { \"file\": string, \"pos\": int }.");
                throw new RuntimeException();
            }

            LOG.info(">>> [MAIN] BINLOG OFFSET = {}, {}", binlogOffsetFile, binlogOffsetPos);
        } else {
            LOG.info(">>> [MAIN] NO BINLOG OFFSET, SNAPSHOT + CDC");
        }

        // <<<

        // BASIC BEST PRACTICE DEBEZIUM CONFIGURATIONS

        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("bigint.unsigned.handling.mode","long");
        debeziumProperties.setProperty("decimal.handling.mode","string");
        debeziumProperties.setProperty("database.history.skip.unparseable.ddl", "false");

        // <<<

        // CREATE FLINK CDC SOURCE

        LOG.info(">>> [MAIN] CREATING FLINK-CDC SOURCE");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname(hostname)
            .port(port)
            .databaseList(databaseName)
            .tableList(databaseName + ".*")
            .username(username)
            .password(password)
            .serverTimeZone(timezone)
            .scanNewlyAddedTableEnabled(true)
            .startupOptions(startupOptions)
            .deserializer(new AvroDebeziumDeserializationSchema())
            .includeSchemaChanges(true)
            .debeziumProperties(debeziumProperties)
            .build();

        // <<<

        // <<<

        // CREATE FLINK SOURCE FROM FLINK CDC SOURCE

        LOG.info(">>> [MAIN] CREATING STREAM FROM FLINK-CDC SOURCE");

        DataStream<String> source = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);

        // MAP TABLE NAME -> OUTPUT TAG FOR SIDE OUTPUT

        LOG.info(">>> [MAIN] CREATING TABLE MAPPING");

        Map<String, Tuple2<OutputTag<String>, Schema>> tableTagSchemaMap = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(String.format("jdbc:mysql://%s:%d?tinyInt1isBit=false", hostname, port), username, password)) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables(databaseName, null, "%", new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables
                    .getString(3);
                String sanitizedTableName = tableName
                    .replace('-', '_');
                if (!tableName.equals(sanitizedTableName)) {
                    LOG.warn(">>> [MAIN] TABLE NAME IS SANITIZED: {} -> {}", tableName, sanitizedTableName);
                }

                String mappedTableName;
                String sanitizedMappedTableName = sanitizedTableName;
                if (tableNameMap != null) {
                    mappedTableName = tableNameMap.getString(tableName);
                    if (mappedTableName != null) {
                        sanitizedMappedTableName = mappedTableName.replace('-', '_');
                    }
                }

                // TODO: MULTIPLE DB?

                ResultSet columns = metaData.getColumns(databaseName, null, tableName, "%");

                SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(sanitizedTableName).fields();
                while (columns.next()) {
                    String columnName = columns
                        .getString("COLUMN_NAME");
                    String sanitizedColumnName = columnName
                        .replace('-', '_');
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

                    fieldAssembler = addNullableFieldToSchema(fieldAssembler, sanitizedColumnName, columnType);
                }

                fieldAssembler = addFieldToSchema(fieldAssembler, "_db", "VARCHAR");
                fieldAssembler = addFieldToSchema(fieldAssembler, "_tbl", "VARCHAR");
                fieldAssembler = addFieldToSchema(fieldAssembler, "_op", "VARCHAR");
                fieldAssembler = addFieldToSchema(fieldAssembler, "_ts", "BIGINT");
                Schema avroSchema = fieldAssembler.endRecord();

                final String outputTagID = String.format("%s__%s", sanitizedDatabaseName, sanitizedMappedTableName);
                final OutputTag<String> outputTag = new OutputTag<>(outputTagID) {};
                tableTagSchemaMap.put(sanitizedTableName, Tuple2.of(outputTag, avroSchema));

                LOG.info(
                    ">>> [MAIN] TABLE-TAG-SCHEMA MAP FOR: {}{}", String.format("%s.%s", sanitizedDatabaseName, sanitizedTableName) ,
                        (
                            !sanitizedTableName.equals(sanitizedMappedTableName) ? ("(" + sanitizedMappedTableName + ")") : ""
                        )
                );
                LOG.info(String.valueOf(avroSchema));
            }
        } catch (SQLException e) {
            LOG.error(">>> [MAIN] UNABLE TO CONNECT TO SOURCE, EXCEPTION:");
            throw e;
        }

        // <<<

        // >>> CAPTURE DDL STATEMENTS TO SPECIAL DDL TABLE

        final String sanitizedDDLTableName = String.format("_%s_ddl", sanitizedDatabaseName);
        SchemaBuilder.FieldAssembler<Schema> ddlFieldAssembler = SchemaBuilder.record(sanitizedDDLTableName).fields();

        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_db", "VARCHAR");
        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_tbl", "VARCHAR");
        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_ddl", "VARCHAR");
        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_ddl_db", "VARCHAR");
        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_ddl_tbl", "VARCHAR");
        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_ts", "BIGINT");
        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_binlog_file", "VARCHAR");
        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_binlog_pos_end", "BIGINT");
        Schema ddlAvroSchema = ddlFieldAssembler.endRecord();

        final String outputTagID = String.format("%s__%s", sanitizedDatabaseName, sanitizedDDLTableName);
        final OutputTag<String> ddlOutputTag = new OutputTag<>(outputTagID) {};
        tableTagSchemaMap.put(sanitizedDDLTableName, Tuple2.of(ddlOutputTag, ddlAvroSchema));

        LOG.info(
            ">>> [MAIN] TABLE-TAG-SCHEMA MAP FOR: {}", String.format("%s.%s", sanitizedDatabaseName, sanitizedDDLTableName)
        );
        LOG.info(String.valueOf(ddlAvroSchema));

        // CHECK FOR DDL STOP SIGNAL
        // NOTE: MUST BE KEYED STREAM TO USE TIMER

        SingleOutputStreamOperator<String> mainDataStream = source
            .keyBy(new NullByteKeySelector<>())
            .process(new DelayedStopSignalProcessFunction())
            .setParallelism(1)
            .keyBy(new NullByteKeySelector<>())
            .process(new StopSignalCheckerProcessFunction(tableTagSchemaMap))
            .setParallelism(1);

        // <<<

        // CREATE SIDE STREAMS

        for (Map.Entry<String, Tuple2<OutputTag<String>, Schema>> entry : tableTagSchemaMap.entrySet()) {
            OutputTag<String> outputTag = entry.getValue().f0;
            Schema avroSchema = entry.getValue().f1;

            SingleOutputStreamOperator<GenericRecord> sideOutputStream = mainDataStream
                .getSideOutput(outputTag)
                .map(new JsonToGenericRecordMapFunction(avroSchema))
                .returns(new GenericRecordAvroTypeInfo(avroSchema));

            ParquetWriterFactory<GenericRecord> compressedParquetWriterFactory = new ParquetWriterFactory<>(
                out -> AvroParquetWriter.<GenericRecord>builder(out).withSchema(avroSchema)
                    .withDataModel(GenericData.get())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()
            );

            Path outputPath = new Path(
                sinkPath + "/" +
                    (!sourceId.isBlank() ? String.format("%s_", sourceId) : "") +
                    outputTag.getId());
            FileSink<GenericRecord> sink = FileSink
                .forBulkFormat(outputPath, compressedParquetWriterFactory)
                .withRollingPolicy(
                    OnCheckpointRollingPolicy
                        .build()
                )
                .withBucketAssigner(new DatabaseTableDateBucketAssigner())
                .build();

            sideOutputStream.sinkTo(sink).setParallelism(1);
        }

        // <<<

        // PRINT FROM MAIN STREAMS
        // TODO: DRYRUN MODE

        mainDataStream
            .print("MAIN ")
            .setParallelism(1);

        // <<<

        // NO RESTART TO STOP AT STOP SIGN

        env.setRestartStrategy(RestartStrategies.noRestart());

        // <<<

        env.execute("Print MySQL Snapshot + Binlog");
    }

    private static SchemaBuilder.FieldAssembler<Schema> addNullableFieldToSchema(
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler,
        String columnName, String columnType
    ) {
        return addFieldToSchema(fieldAssembler, columnName, columnType, true);
    }

    private static SchemaBuilder.FieldAssembler<Schema> addFieldToSchema(
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler,
        String columnName, String columnType
    ) {
        return addFieldToSchema(fieldAssembler, columnName, columnType, false);
    }

    private static SchemaBuilder.FieldAssembler<Schema> addFieldToSchema(
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler,
        String columnName, String columnType, boolean isNullable
    ) {
        // REMOVE "UNSIGNED" OR OTHER NASTY THINGS
        columnType = columnType.replaceFirst(" .*", "");

        switch (columnType.toUpperCase()) {
            case "INT":
            case "TINYINT":
            case "SMALLINT":
            case "MEDIUMINT":
            case "DATE":
                fieldAssembler = isNullable
                    ? fieldAssembler.name(columnName).type().unionOf().nullType().and().intType().endUnion().nullDefault()
                    : fieldAssembler.name(columnName).type().intType().noDefault();
                break;
            case "BIGINT":
            case "DATETIME":
            case "TIME":
                fieldAssembler = isNullable
                    ? fieldAssembler.name(columnName).type().unionOf().nullType().and().longType().endUnion().nullDefault()
                    : fieldAssembler.name(columnName).type().longType().noDefault();
                break;
            case "FLOAT":
            case "DOUBLE":
                fieldAssembler = isNullable
                    ? fieldAssembler.name(columnName).type().unionOf().nullType().and().doubleType().endUnion().nullDefault()
                    : fieldAssembler.name(columnName).type().doubleType().noDefault();
                break;
            case "BIT":
            case "BOOL":
            case "BOOLEAN":
                fieldAssembler = isNullable
                    ? fieldAssembler.name(columnName).type().unionOf().nullType().and().booleanType().endUnion().nullDefault()
                    : fieldAssembler.name(columnName).type().booleanType().noDefault();
                break;
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
            case "DECIMAL":
            case "TIMESTAMP":
            default:
                fieldAssembler = isNullable
                    ? fieldAssembler.name(columnName).type().unionOf().nullType().and().stringType().endUnion().nullDefault()
                    : fieldAssembler.name(columnName).type().stringType().noDefault();
                break;
        }

        return fieldAssembler;
    }

    public static class JsonToGenericRecordMapFunction extends RichMapFunction<String, GenericRecord> {
        private final String schemaString;
        private transient Schema avroSchema;

        public JsonToGenericRecordMapFunction(Schema schema) {
            this.schemaString = schema.toString();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.avroSchema = new Schema.Parser().parse(schemaString);
        }

        @Override
        public GenericRecord map(String value) throws Exception {
            LOG.debug(">>> [MAIN] CONVERT JSON STRING TO AVRO");
            LOG.trace(value);
            LOG.trace(avroSchema);
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
            Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema, value);
            return reader.read(null, decoder);
        }
    }
}
