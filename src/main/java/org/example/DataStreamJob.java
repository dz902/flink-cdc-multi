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
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.connector.file.sink.FileSink;
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

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class DataStreamJob {
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);

    public static void main(String[] args) throws Exception {
        // YOU MUST MANUALLY LOAD CONFIG FOR S3 REGION TO TAKE EFFECT

        Configuration configuration = GlobalConfiguration.loadConfiguration();
        FileSystem.initialize(configuration, null);

        // <<<

        // CLI OPTIONS >>>
        Options options = new Options();
        options.addOption("c", "config", true, "config json");

        String configJsonStr = """
            {
                "sink.path": "s3://0000-flink-cdc/data/ods",
                "source.id": "mysource",
                "source.database.name": "test",
                "binlog.offset": {
                    "file": "mysql-bin.000003",
                    "pos": 39570
                },
                "table.name.map": {
                    "booka": "booka_v20240713"
                }
            }
            """;
        final String argConfig = options.getOption("c").getValue(configJsonStr);

        JSONObject tableNameMap = null;
        JSONObject binlogOffset = null;
        String sinkPath = "";
        String sourceId = "";
        String databaseName = "";

        if (argConfig != null) {
            JSONObject config = null;
            try {
                config = JSONObject.parseObject(argConfig);
            } catch (Exception e) {
                System.err.println("[ERROR] Config JSON is not valid.");
                System.exit(1);
            }

            tableNameMap = config.getJSONObject("table.name.map");
            binlogOffset = config.getJSONObject("binlog.offset");
            sinkPath = config.getString("sink.path");
            sourceId = config.getString("source.id");
            databaseName = config.getString("source.database.name");
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
                System.err.println("[ERROR] Binlog offset format must be: { \"file\": string, \"pos\": int }.");
                System.exit(1);
            }
        }

        // <<<

        // BASIC BEST PRACTICE DEBEZIUM CONFIGURATIONS

        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("bigint.unsigned.handling.mode","long");
        debeziumProperties.setProperty("decimal.handling.mode","string");

        // <<<

        // CREATE FLINK CDC SOURCE

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("localhost")
            .port(63306)
            .databaseList(databaseName)
            .tableList(databaseName + ".*")
            .username("root")
            .password("testtest")
            .serverTimeZone("UTC")
            .scanNewlyAddedTableEnabled(true)
            .startupOptions(startupOptions)
            .deserializer(new AvroDebeziumDeserializationSchema())
            .includeSchemaChanges(true)
            .debeziumProperties(debeziumProperties)
            .build();

        // <<<

        // FLINK ENV SETUP

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);

        // <<<

        // CREATE FLINK SOURCE FROM FLINK CDC SOURCE

        DataStream<String> source = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);

        // MAP TABLE NAME -> OUTPUT TAG FOR SIDE OUTPUT

        Map<String, Tuple2<OutputTag<String>, Schema>> tableTagSchemaMap = new HashMap<>();
        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:63306", "root", "testtest")) {
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

                final String outputTagID = String.format("%s/%s", databaseName, mappedTableName);
                final OutputTag<String> outputTag = new OutputTag<>(outputTagID) {};
                tableTagSchemaMap.put(tableName, Tuple2.of(outputTag, avroSchema));

                System.out.println(
                    ">>>> TABLE-TAG-SCHEMA MAPPING FOR " + outputTagID +
                        (
                            tableName.equals(mappedTableName) ? ("(" + mappedTableName + ")") : ""
                        )
                );
                System.out.println(avroSchema);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.exit(1);
            //e.printStackTrace();
        }

        // <<<

        // >>> CAPTURE DDL STATEMENTS TO SPECIAL DDL TABLE

        final String tableName = String.format("%s_ddl", databaseName);
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

        final OutputTag<String> ddlOutputTag = new OutputTag<>(String.format("%s/%s", databaseName, tableName)) {};
        tableTagSchemaMap.put(tableName, Tuple2.of(ddlOutputTag, ddlAvroSchema));

        // CHECK FOR DDL STOP SIGNAL
        // NOTE: MUST BE KEYED STREAM TO USE TIMER

        SingleOutputStreamOperator<String> mainDataStream = source
            .keyBy(new NullByteKeySelector<>())
            .process(new DelayedStopSignalProcessFunction(tableTagSchemaMap, ddlOutputTag))
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
                    (!sourceId.isBlank() ? String.format("%s/", sourceId) : "") +
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
