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
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;

public class DataStreamJob {
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);

    public static void main(String[] args) throws Exception {
        // YOU MUST MANUALLY LOAD CONFIG FOR S3 REGION TO TAKE EFFECT

        Configuration configuration = GlobalConfiguration.loadConfiguration();
        FileSystem.initialize(configuration, null);

        // <<<

        // CLI OPTIONS >>>
        LOG.info(">>> [APP] ARGS: {}", Arrays.toString(args));

        Options options = new Options();
        options.addOption("c", "config", true, "config json");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

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
        int checkpointInterval = 30;

        // TODO: WRITE CONFIGURATION BACK
        // TODO: ADD STATS TABLE

        if (argConfig != null) {
            LOG.info(">>> [CONFIG] LOADING CONFIG FROM {}", argConfig);

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

            JSONObject config;
            try {
                config = JSONObject.parseObject(configJSONString);
            } catch (Exception e) {
                // do not print json contents as it may contain credentials
                LOG.error(">>> [CONFIG] CONFIG JSON IS NOT VALID");
                throw new RuntimeException();
            }

            tableNameMap = config.getJSONObject("table.name.map");

            if (tableNameMap != null) {
                LOG.info(">>> [CONFIG] LOADED TABLE NAME MAP: {}", tableNameMap);
            }

            binlogOffset = config.getJSONObject("binlog.offset");
            sinkPath = config.getString("sink.path");
            sourceId = config.getString("source.id");
            hostname = Objects.requireNonNullElse(config.getString("source.hostname"), hostname);
            port = config.getIntValue("source.port") > 0 ? config.getIntValue("source.port") : port;
            username = Objects.requireNonNullElse(config.getString("source.username"), username);
            password = Objects.requireNonNullElse(config.getString("source.password"), password);
            databaseName = config.getString("source.database.name");
            timezone = Objects.requireNonNullElse(config.getString("source.timezone"), timezone);
            checkpointInterval = config.getIntValue("checkpoint.interval") > 0 ? config.getIntValue("checkpoint.interval") : checkpointInterval;

            LOG.info(">>> [CONFIG] LOADED SINK PATH: {}", sinkPath);
            LOG.info(">>> [CONFIG] LOADED SOURCE: {}:{}", hostname, port);
        } else {
            LOG.info(">>> NO CONFIG PROVIDED");
        }

        // <<<

        // BINLOG OFFSET

        StartupOptions startupOptions = StartupOptions.initial();
        if (binlogOffset != null) {
            String binlogOffsetFile = binlogOffset.getString("file");
            long binlogOffsetPos = binlogOffset.getLongValue("pos");

            if (binlogOffsetFile != null && binlogOffsetPos > 0) {
                startupOptions = StartupOptions.specificOffset(binlogOffsetFile, binlogOffsetPos);
            } else {
                LOG.error(">>> [CONFIG] BINLOG OFFSET NOT IN FORMAT: { \"file\": string, \"pos\": int }.");
                throw new RuntimeException();
            }

            LOG.info(">>> [CONFIG] BINLOG OFFSET = {}, {}", binlogOffsetFile, binlogOffsetPos);
        } else {
            LOG.info(">>> [CONFIG] NO BINLOG OFFSET, SNAPSHOT + CDC");
        }

        // <<<

        // BASIC BEST PRACTICE DEBEZIUM CONFIGURATIONS

        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("bigint.unsigned.handling.mode","long");
        debeziumProperties.setProperty("decimal.handling.mode","string");

        // <<<

        // CREATE FLINK CDC SOURCE

        LOG.info(">>> [FLINK-CDC] CREATING FLINK-CDC SOURCE");

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

        // FLINK ENV SETUP

        LOG.info(">>> [FLINK] SETTING UP ENV");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(checkpointInterval * 1000L);

        // <<<

        // CREATE FLINK SOURCE FROM FLINK CDC SOURCE

        LOG.info(">>> [FLINK] CREATING STREAM FROM FLINK-CDC SOURCE");

        DataStream<String> source = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);

        // MAP TABLE NAME -> OUTPUT TAG FOR SIDE OUTPUT

        LOG.info(">>> [APP] CREATING TABLE MAPPING");

        Map<String, Tuple2<OutputTag<String>, Schema>> tableTagSchemaMap = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(String.format("jdbc:mysql://%s:%d?tinyInt1isBit=false", hostname, port), username, password)) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables(databaseName, null, "%", new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables.getString(3);
                String mappedTableName = tableName;
                if (tableNameMap != null) {
                    mappedTableName = Objects.requireNonNullElse(tableNameMap.getString(tableName), tableName);
                    //tableNameMap.getString(tableName) != null ? tableNameMap.getString(tableName) : tableName;
                }

                ResultSet columns = metaData.getColumns(databaseName, null, tableName, "%");

                SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(tableName).fields();
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    String columnType = columns.getString("TYPE_NAME");
                    //boolean isNullable = columns.getInt("NULLABLE") == 1;

                    // NOTE: NULL is always allowed

                    fieldAssembler = addNullableFieldToSchema(fieldAssembler, columnName, columnType);
                }

                fieldAssembler = addFieldToSchema(fieldAssembler, "_db", "VARCHAR");
                fieldAssembler = addFieldToSchema(fieldAssembler, "_tbl", "VARCHAR");
                fieldAssembler = addFieldToSchema(fieldAssembler, "_op", "VARCHAR");
                fieldAssembler = addFieldToSchema(fieldAssembler, "_ts", "BIGINT");
                Schema avroSchema = fieldAssembler.endRecord();

                final String outputTagID = String.format("%s__%s", databaseName, mappedTableName);
                final OutputTag<String> outputTag = new OutputTag<>(outputTagID) {};
                tableTagSchemaMap.put(tableName, Tuple2.of(outputTag, avroSchema));

                LOG.info(
                    ">>> [APP] TABLE-TAG-SCHEMA MAP FOR: {}{}", String.format("%s.%s", databaseName, tableName) ,
                        (
                            tableName.equals(mappedTableName) ? ("(" + mappedTableName + ")") : ""
                        )
                );
                LOG.info(String.valueOf(avroSchema));
            }
        } catch (SQLException e) {
            LOG.error(">>> [APP] UNABLE TO CONNECT TO SOURCE, EXCEPTION:");
            throw e;
        }

        // <<<

        // >>> CAPTURE DDL STATEMENTS TO SPECIAL DDL TABLE

        final String tableName = String.format("_%s_ddl", databaseName);
        SchemaBuilder.FieldAssembler<Schema> ddlFieldAssembler = SchemaBuilder.record(tableName).fields();

        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_db", "VARCHAR");
        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_tbl", "VARCHAR");
        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_ddl", "VARCHAR");
        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_ddl_db", "VARCHAR");
        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_ddl_tbl", "VARCHAR");
        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_ts", "BIGINT");
        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_binlog_file", "VARCHAR");
        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_binlog_pos_end", "BIGINT");
        Schema ddlAvroSchema = ddlFieldAssembler.endRecord();

        final OutputTag<String> ddlOutputTag = new OutputTag<>(tableName) {};
        tableTagSchemaMap.put(tableName, Tuple2.of(ddlOutputTag, ddlAvroSchema));

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
        // TODO: MAKE THIS OPTIONAL, NOT NEEDED IN PRODUCTION, HUGE LOGS
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

    private static SchemaBuilder.FieldAssembler<Schema> addFieldToSchema(
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler,
        String columnName, String columnType
    ) {
        switch (columnType.toUpperCase()) {
            case "INT":
            case "TINYINT":
            case "SMALLINT":
            case "MEDIUMINT":
            case "DATE":
                fieldAssembler = fieldAssembler.name(columnName).type().intType().noDefault();
                break;
            case "BIGINT":
            case "DATETIME":
            case "TIME":
                fieldAssembler = fieldAssembler.name(columnName).type().longType().noDefault();
                break;
            case "FLOAT":
            case "DOUBLE":
                fieldAssembler = fieldAssembler.name(columnName).type().doubleType().noDefault();
                break;
            case "BIT": // TODO: MULTIPLE BITS TREATMENT
            case "BOOL":
            case "BOOLEAN":
                fieldAssembler = fieldAssembler.name(columnName).type().booleanType().noDefault();
                break;
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
            case "DECIMAL":
            case "TIMESTAMP":
            default:
                fieldAssembler = fieldAssembler.name(columnName).type().stringType().noDefault();
                break;
        }

        return fieldAssembler;
    }

    // >>> CONVERT DATABASE COLUMN TYPE TO AVRO TYPE

    private static SchemaBuilder.FieldAssembler<Schema> addNullableFieldToSchema(
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler,
        String columnName, String columnType
    ) {
        switch (columnType.toUpperCase()) {
            case "INT":
            case "TINYINT":
            case "SMALLINT":
            case "MEDIUMINT":
            case "DATE":
                fieldAssembler = fieldAssembler.name(columnName).type().unionOf().nullType().and().intType().endUnion().nullDefault();
                break;
            case "BIGINT":
            case "DATETIME":
            case "TIME":
                fieldAssembler = fieldAssembler.name(columnName).type().unionOf().nullType().and().longType().endUnion().nullDefault();
                break;
            case "FLOAT":
            case "DOUBLE":
                fieldAssembler = fieldAssembler.name(columnName).type().unionOf().nullType().and().doubleType().endUnion().nullDefault();
                break;
            case "DECIMAL": // Note: decimal.handling.mode = string
            case "TIMESTAMP":
                fieldAssembler = fieldAssembler.name(columnName).type().unionOf().nullType().and().stringType().endUnion().nullDefault();
                break;
            case "BIT":
            case "BOOL":
            case "BOOLEAN":
                fieldAssembler = fieldAssembler.name(columnName).type().unionOf().nullType().and().booleanType().endUnion().nullDefault();
                break;
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
            default:
                fieldAssembler = fieldAssembler.name(columnName).type().unionOf().nullType().and().stringType().endUnion().nullDefault();
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
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
            Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema, value);
            return reader.read(null, decoder);
        }
    }
}
