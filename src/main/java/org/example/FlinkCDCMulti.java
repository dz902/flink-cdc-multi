package org.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.example.bucketassigners.DatabaseTableDateBucketAssigner;
import org.example.processfunctions.mongodb.TimestampOffsetStoreProcessFunction;
import org.example.richmapfunctions.JSONToGenericRecordMapFunction;
import org.example.sinkfunctions.SingleFileSinkFunction;
import org.example.streamers.MongoStreamer;
import org.example.streamers.MySQLStreamer;
import org.example.streamers.Streamer;
import org.example.utils.Thrower;
import org.example.utils.Validator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class FlinkCDCMulti {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private static final Configuration flinkConfig = GlobalConfiguration.loadConfiguration();
    private static final Options options = new Options();
    private static String argConfig;
    private static Boolean argDebugMode = false;
    private static JSONObject configJSON = new JSONObject();
    private static String sourceType;
    private static String sourceId;
    private static String sinkPath;
    private static String offsetValue;
    private static String offsetStorePath;
    private static String offsetStoreFilePath;
    private static Streamer<String> streamer;
    private static DataStream<String> sourceStream;
    private static StreamExecutionEnvironment env;
    private static Map<String, Tuple2<OutputTag<String>, Schema>> tagSchemaMap;
    private static JobExecutionResult jobExecutionResult;

    public static void main(String[] args) throws Exception {
        createFlinkStreamingEnv();

        initializeFileSystem();
        processCLIOptions(args);

        enableDebugMode(argDebugMode);
        loadConfigJSON(argConfig);

        configureCheckpointing();
        configureTableNameMap();
        configureOffset();

        createStreamer();
        createTagSchemaMap();
        createSourceStream();
        createSideStreams();
        createOffsetStoreStream();

        addDefaultPrintSink();
        setFlinkRestartStrategy();

        startFlinkJob();
    }

    private static void createTagSchemaMap() {
        tagSchemaMap = streamer.createTagSchemaMap();
    }

    private static void createFlinkStreamingEnv() {
        LOG.info(">>> [MAIN] SETTING UP FLINK ENV");
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    private static void createSourceStream() {
        Source<String, ?, ?> source = streamer.getSource();
        sourceStream = env
            .fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                streamer.getClass().getSimpleName()
            )
            .setParallelism(1);
    }

    private static void addDefaultPrintSink() {
        if (argDebugMode) {
            sourceStream
                .print("[DEBUG-SINK] > ")
                .setParallelism(1);
        }
    }

    private static void setFlinkRestartStrategy() {
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    private static void startFlinkJob() throws Exception {
        jobExecutionResult = env.execute("JOB-" + sourceType);

    }

    private static void configureOffset() throws IOException {
        offsetValue = configJSON.getString("offset.value");

        if (!StringUtils.isNullOrWhitespaceOnly(offsetValue)) {
            LOG.info(">>> [MAIN] OFFSET VALUE: {}", offsetValue);
            return;
        }

        String sourceId = Validator.ensureNotEmpty("source.id", configJSON.getString("source.id"));

        offsetStoreFilePath = configJSON.getString("offset.store.file.path");

        if (StringUtils.isNullOrWhitespaceOnly(offsetStoreFilePath)) {
            if (StringUtils.isNullOrWhitespaceOnly(offsetStorePath)) {
                LOG.info(">>> [MAIN] NO OFFSET STORE PATH SET, FEATURE DISABLED");
                return;
            } else {
                offsetStoreFilePath = String.format("%s/%s_offset.txt", offsetStorePath, sourceId);
            }
        }

        LOG.info(">>> [MAIN] OFFSET STORE PATH: {}", offsetStoreFilePath);
        LOG.info(">>> [MAIN] LOADING OFFSET FROM PATH: {}", offsetStoreFilePath);

        Path storeFilePath = new Path(offsetStoreFilePath);

        FileSystem storeFS;
        try {
            storeFS = storeFilePath.getFileSystem();
        } catch (IOException e) {
            Thrower.errAndThrow(
                "MAIN",
                String.format("INVALID OFFSET STORE PATH: %s", offsetStoreFilePath)
            );
            return;
        }

        String offsetValueFromStore = "";

        try (FSDataInputStream storeInputStream = storeFS.open(storeFilePath)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(storeInputStream));

            StringBuilder content = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }

            offsetValueFromStore = content.toString().trim();
        } catch (FileNotFoundException e) {
            LOG.info(">>> [MAIN] OFFSET STORE DOES NOT EXIST, SNAPSHOT + CDC");
        } catch (Exception e) {
            LOG.error(">>> [MAIN] OFFSET STORE ERROR");
            LOG.error(FlinkCDCMulti.offsetValue);
            throw e;
        }

        if (StringUtils.isNullOrWhitespaceOnly(offsetValueFromStore)) {
            LOG.info(">>> [MAIN] EMPTY OFFSET STORE, SNAP + CDC");
            return;
        }

        LOG.info(">>> [MAIN] USING OFFSET: {}", offsetValueFromStore);
        configJSON.put("offset.value", offsetValueFromStore);
    }

    private static void createStreamer() {
        if (sourceType == null || sourceType.isBlank()) {
            String msg = "SOURCE TYPE NOT SPECIFIED";
            LOG.error(">>> [MAIN] {}", msg);
            throw new RuntimeException(msg);
        }

        switch (sourceType) {
            case "mongo":
                streamer = new MongoStreamer(configJSON);
                break;
            case "mysql":
                streamer = new MySQLStreamer(configJSON);
                break;
            default:
                String msg = String.format("UNSUPPORTED SOURCE TYPE: %s", sourceType);
                LOG.error(">>> [MAIN] {}", msg);
                throw new RuntimeException(msg);
        }
    }

    private static void createOffsetStoreStream() {
        if (StringUtils.isNullOrWhitespaceOnly(offsetStorePath)) {
            return;
        }

        LOG.info(">>> [MAIN] CREATING OFFSET STREAM");

        sourceStream
            .keyBy(new NullByteKeySelector<>())
            .process(new TimestampOffsetStoreProcessFunction())
            .setParallelism(1)
            .addSink(new SingleFileSinkFunction(new Path(offsetStoreFilePath)))
            .setParallelism(1);
    }

    private static void createSideStreams() {
        SingleOutputStreamOperator<String> mainDataStream = streamer.createMainDataStream(sourceStream);

        for (Map.Entry<String, Tuple2<OutputTag<String>, Schema>> entry : tagSchemaMap.entrySet()) {
            OutputTag<String> outputTag = entry.getValue().f0;
            Schema avroSchema = entry.getValue().f1;

            SingleOutputStreamOperator<GenericRecord> sideOutputStream = mainDataStream
                .getSideOutput(outputTag)
                .map(new JSONToGenericRecordMapFunction(avroSchema))
                .setParallelism(1)
                .returns(new GenericRecordAvroTypeInfo(avroSchema));

            ParquetWriterFactory<GenericRecord> compressedParquetWriterFactory = new ParquetWriterFactory<>(
                out -> AvroParquetWriter.<GenericRecord>builder(out).withSchema(avroSchema)
                    .withDataModel(GenericData.get())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()
            );

            Path outputPath = new Path(
                String.format("%s/%s_%s", sinkPath, sourceId, outputTag.getId())
            );
            FileSink<GenericRecord> sink = FileSink
                .forBulkFormat(outputPath, compressedParquetWriterFactory)
                .withRollingPolicy(
                    OnCheckpointRollingPolicy
                        .build()
                )
                .withBucketAssigner(new DatabaseTableDateBucketAssigner())
                .build();

            sideOutputStream
                .sinkTo(sink)
                .setParallelism(1);
        }
    }

    private static void processCLIOptions(String[] args) throws IOException, ParseException {
        LOG.info(">>> [MAIN] ARGS: {}", Arrays.toString(args));

        Options options = new Options();
        options.addOption("c", "config", true, "config json");
        options.addOption(null, "debug", false, "Enable debug logging.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            LOG.error(">>> [MAIN] INVALID CLI OPTIONS");
            LOG.error(args);
            throw e;
        }

        argDebugMode = cmd.hasOption("debug");
        argConfig = cmd.getOptionValue("c");
    }

    private static void configureCheckpointing() {
        int checkpointInterval = configJSON.getIntValue("checkpoint.interval") > 0 ? configJSON.getIntValue("checkpoint.interval") : 30;
        String checkpointStorage = Objects.requireNonNullElse(configJSON.getString("checkpoint.storage"), "jobmanager");
        String checkpointDirectory;

        if (checkpointStorage.equals("jobmanager") || checkpointStorage.equals("filesystem")) {
            LOG.info(">>> [MAIN] LOADED CHECKPOINT STORAGE: {}", checkpointStorage);

            if (checkpointStorage.equals("filesystem")) {

                checkpointDirectory = configJSON.getString("checkpoint.directory");

                if (checkpointDirectory == null) {
                    LOG.error(">>> [MAIN] CHECKPOINT DIRECTORY IS EMPTY FOR FILESYSTEM STORAGE");
                    throw new RuntimeException();
                }

                flinkConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, checkpointStorage);
                flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDirectory);
            } else {
                LOG.warn(">>> [MAIN] CHECKPOINT STORAGE IN JOBMANAGER IS NOT RECOMMENDED FOR PRODUCTION");
            }
        } else {
            Thrower.errAndThrow("MAIN",">>> [MAIN] CHECKPOINT STORAGE NOT IN FORMAT: filesystem | jobmanager");
        }


        LOG.info(">>> [MAIN] CHECKPOINT INTERVAL: {}", checkpointInterval);

        env.configure(flinkConfig);
        env.enableCheckpointing(checkpointInterval * 1000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
    }

    private static void enableDebugMode(Boolean argDebugMode) {
        if (argDebugMode) {
            LOG.info(">>> [MAIN] DEBUG MODE");

            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();

            LoggerConfig loggerConfig = new LoggerConfig("flink-cdc-multi", Level.TRACE, true);
            config.addLogger("flink-cdc-multi", loggerConfig);

            ctx.updateLoggers();
        }
    }

    private static void loadConfigJSON(String argConfig) throws IOException {
        if (argConfig != null && !argConfig.isBlank()) {
            LOG.info(">>> [MAIN] LOADING CONFIG FROM {}", argConfig);

            Path configPath = new Path(argConfig);

            FileSystem configFS;
            try {
                configFS = configPath.getFileSystem();
            } catch (IOException e) {
                LOG.error(">>> [MAIN] INVALID CONFIG PATH: {}", argConfig);
                throw e;
            }

            String configJSONString = "";

            try (FSDataInputStream configInputStream = configFS.open(configPath)) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(configInputStream));

                StringBuilder content = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    content.append(line).append("\n");
                }

                configJSONString = content.toString();
            } catch (Exception e) {
                LOG.error(">>> [MAIN] CONFIG FILE ERROR");
                LOG.error(configJSONString);
                throw e;
            }

            try {
                configJSON = JSONObject.parseObject(configJSONString);
            } catch (Exception e) {
                // do not print json contents as it may contain credentials
                LOG.error(">>> [MAIN] CONFIG JSON IS NOT VALID");
                throw new RuntimeException();
            }
        } else {
            String msg = "CONFIG FILE NOT PROVIDED";
            LOG.error(">>> [MAIN] {}", msg);
            throw new RuntimeException(msg);
        }

        sourceId = Validator.ensureNotEmpty("source.id", configJSON.getString("source.id"));
        sourceType = Validator.ensureNotEmpty("source.type", configJSON.getString("source.type"));
        sinkPath = Validator.ensureNotEmpty("sink.path", configJSON.getString("sink.path"));
    }

    private static void configureTableNameMap() {
        JSONObject tableNameMap = configJSON.getJSONObject("table.name.map");

        if (tableNameMap != null) {
            LOG.info(">>> [MAIN] LOADED TABLE NAME MAP: {}", tableNameMap);
        }
    }

    private static void initializeFileSystem() {
        // YOU MUST MANUALLY LOAD CONFIG FOR S3 REGION TO TAKE EFFECT
        // TODO: FIND OUT WHY
        PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(flinkConfig);
        FileSystem.initialize(flinkConfig, pluginManager);
    }
}
