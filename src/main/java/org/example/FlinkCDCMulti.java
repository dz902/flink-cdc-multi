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
import org.apache.flink.api.java.utils.ParameterTool;
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
import org.example.bucketassigners.DateBucketAssigner;
import org.example.processfunctions.common.StatusStoreProcessFunction;
import org.example.processfunctions.mongodb.TimestampOffsetStoreProcessFunction;
import org.example.richmapfunctions.JSONToGenericRecordMapFunction;
import org.example.sinkfunctions.SingleFileSinkFunction;
import org.example.streamers.MongoDBStreamer;
import org.example.streamers.MySQLStreamer;
import org.example.streamers.Streamer;
import org.example.utils.Thrower;
import org.example.utils.Validator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class FlinkCDCMulti {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private static final Configuration flinkConfig = GlobalConfiguration.loadConfiguration();
    private static final Options options = new Options();
    private static String argConfig;
    private static String argName;
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
    private static DataStream<GenericRecord> sideStreams;
    private static StreamExecutionEnvironment env;
    private static Map<String, Tuple2<OutputTag<String>, Schema>> tagSchemaMap;
    private static JobExecutionResult jobExecutionResult;

    public static void main(String[] args) throws Exception {
        LOG.info(">>> [MAIN] VERSION: {}", "v20240726-1557");

        createFlinkStreamingEnv();

        initializeFileSystem();
        processCLIArgs(args);

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
        createStatusStoreStream();

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
        String jobName = null;

        if (StringUtils.isNullOrWhitespaceOnly(argName)) {
            jobName = argName;
            LOG.info(">>> [MAIN] JOB NAME FROM CLI ARGS: {}", jobName);
        }

        String configJobName = configJSON.getString("job.name");
        if (StringUtils.isNullOrWhitespaceOnly(configJobName)) {
            jobName = configJobName;
            LOG.info(">>> [MAIN] JOB NAME FROM CONFIG FILE: {}", jobName);
        }

        if (StringUtils.isNullOrWhitespaceOnly(jobName)) {
            jobName = "JOB-" + sourceId;
            LOG.info(">>> [MAIN] JOB NAME NOT, USING DEFAULT: {}", jobName);
        }

        final Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("jobName", jobName);

        final ParameterTool params = ParameterTool.fromMap(paramsMap);
        env.getConfig().setGlobalJobParameters(params);

        jobExecutionResult = env.execute(jobName);
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
            offsetStorePath = configJSON.getString("offset.store.path");

            if (StringUtils.isNullOrWhitespaceOnly(offsetStorePath)) {
                LOG.info(">>> [MAIN] OFFSET STORE CONFIG NOT FOUND, FEATURE DISABLED");
                return;
            } else {
                offsetStoreFilePath = String.format("%s/%s_offset.txt", offsetStorePath, sourceId);
                LOG.info(">>> [MAIN] GOT OFFSET STORE PATH: {}", offsetStorePath);
                LOG.info(">>> [MAIN] COMPUTED OFFSET STORE FILE PATH: {}", offsetStoreFilePath);
            }
        } else {
            LOG.info(">>> [MAIN] GOT OFFSET STORE FILE PATH: {}", offsetStoreFilePath);
        }

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

        String offsetValueFromStore = null;

        try (FSDataInputStream storeInputStream = storeFS.open(storeFilePath)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(storeInputStream));

            StringBuilder content = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }

            offsetValueFromStore = content.toString().trim();
        } catch (FileNotFoundException e) {
            LOG.info(">>> [MAIN] OFFSET STORE FILE DOES NOT EXIST");
            return;
        } catch (Exception e) {
            LOG.error(">>> [MAIN] OFFSET STORE ERROR: {}", FlinkCDCMulti.offsetValue);
            throw e;
        }

        if (offsetValueFromStore != null && offsetValueFromStore.isBlank()) {
            LOG.info(">>> [MAIN] OFFSET STORE FILE IS EMPTY");
            return;
        }

        LOG.info(">>> [MAIN] OFFSET READ: {}", offsetValueFromStore);
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
                streamer = new MongoDBStreamer(configJSON);
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
        if (StringUtils.isNullOrWhitespaceOnly(offsetStoreFilePath)) {
            return;
        }

        LOG.info(">>> [MAIN] CREATING OFFSET STREAM: {}", offsetStoreFilePath);

        sourceStream
            .keyBy(new NullByteKeySelector<>())
            .process(new TimestampOffsetStoreProcessFunction())
            .setParallelism(1)
            .addSink(new SingleFileSinkFunction(new Path(offsetStoreFilePath)))
            .setParallelism(1);
    }

    private static void createStatusStoreStream() {
        String statusStorePath = configJSON.getString("status.store.path");

        if (StringUtils.isNullOrWhitespaceOnly(statusStorePath)) {
            LOG.info(">>> [MAIN] STATUS STORE PATH NOT SET, FEATURE DISABLED");
            return;
        }

        // TODO: EXTRACT THIS TO UTILS
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());
        String dateString = dateFormatter.format(Instant.ofEpochMilli(System.currentTimeMillis()));
        String statusStoreFilePath = String.format("%s/dt=%s/%s", statusStorePath, dateString, UUID.randomUUID() + ".json");

        LOG.info(">>> [MAIN] CREATING STATUS STORE STREAM: {}", statusStorePath);

        sourceStream
            .keyBy(new NullByteKeySelector<>())
            .process(new StatusStoreProcessFunction())
            .setParallelism(1)
            .addSink(new SingleFileSinkFunction(new Path(statusStoreFilePath)))
            .setParallelism(1);
    }

    private static void createSideStreams() {
        SingleOutputStreamOperator<String> mainDataStream = streamer.createMainDataStream(sourceStream);

        for (Map.Entry<String, Tuple2<OutputTag<String>, Schema>> entry : tagSchemaMap.entrySet()) {
            OutputTag<String> outputTag = entry.getValue().f0;
            Schema avroSchema = entry.getValue().f1;

            SingleOutputStreamOperator<GenericRecord> sideStream = mainDataStream
                .getSideOutput(outputTag)
                .map(new JSONToGenericRecordMapFunction(avroSchema))
                .setParallelism(1)
                .returns(new GenericRecordAvroTypeInfo(avroSchema));

            if (sideStreams == null) {
                sideStreams = sideStream;
            } else {
                // TODO: RECORD COUNTING BY TABLE
                //sideStreams = sideStreams.union(sideStream);
            }

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
                .withBucketAssigner(new DateBucketAssigner())
                .build();

            sideStream
                .sinkTo(sink)
                .setParallelism(1);
        }
    }

    private static void processCLIArgs(String[] args) throws IOException, ParseException {
        LOG.info(">>> [MAIN] ARGS: {}", Arrays.toString(args));

        Options options = new Options();
        options.addOption("c", "config", true, "Path to config JSON file");
        options.addOption("n", "name", true, "Job name");
        options.addOption(null, "debug", false, "Enable debug logging");

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
        argName = cmd.getOptionValue("n");
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
