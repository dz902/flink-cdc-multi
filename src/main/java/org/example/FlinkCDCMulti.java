package org.example;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.example.processfunctions.TimestampOffsetStoreProcessFunction;
import org.example.sinkfunctions.SingleFileSinkFunction;
import org.example.streamers.MongoStreamer;
import org.example.streamers.Streamer;
import org.example.utils.Thrower;
import org.example.utils.Validator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Objects;

public class FlinkCDCMulti {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private static final Configuration flinkConfig = GlobalConfiguration.loadConfiguration();
    private static final Options options = new Options();
    private static String argConfig;
    private static Boolean argDebugMode = false;
    private static JSONObject configJSON = new JSONObject();
    private static String sourceType;
    private static JSONObject tableNameMap = new JSONObject();
    private static String offsetValue;
    private static Streamer streamer;
    private static DataStream<String> sourceStream;
    private static DataStream<String> offsetStream;
    private static StreamExecutionEnvironment env;

    public static void main(String[] args) throws Exception {
        createFlinkStreamingEnv();

        initializeFileSystem();
        processCLIOptions(args);

        enableDebugMode(argDebugMode);
        loadConfigJSON(argConfig);

        configureCheckpoint();
        configureTableNameMap();
        configureOffset();

        createStreamer();
        fetchSchema();
        createSourceStream();
        createOffsetStoreStream();

        addDefaultPrintSink();
        setFlinkRestartStrategy();

        startFlinkJob();
    }

    private static void fetchSchema() {
        streamer.getAvroSchemaMap();
        System.exit(1);
    }

    private static void createFlinkStreamingEnv() {
        LOG.info(">>> [MAIN] SETTING UP FLINK ENV");
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    private static void createSourceStream() {
        Source<String, SourceSplitBase, PendingSplitsState> source = streamer.getSource();
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
        env.execute("JOB-" + sourceType);
    }

    private static void configureOffset() throws IOException {
        offsetValue = configJSON.getString("offset.value");

        if (!StringUtils.isNullOrWhitespaceOnly(offsetValue)) {
            LOG.info(">>> [MAIN] OFFSET VALUE: {}", offsetValue);
            return;
        }

        String offsetStorePath = configJSON.getString("offset.store.path");
        String sourceId = Validator.ensureNotEmpty("source.id", configJSON.getString("source.id"));

        if (StringUtils.isNullOrWhitespaceOnly(offsetStorePath)) {
            LOG.info(">>> [MAIN] NO OFFSET PATH SET");
        }

        String offsetStoreFilePath = offsetStorePath + String.format("/%s_offset.txt", sourceId);

        LOG.info(">>> [MAIN] LOADING OFFSET FROM PATH: {}", offsetStoreFilePath);

        Path storeFilePath = new Path(offsetStoreFilePath);

        FileSystem storeFS = null;
        try {
            storeFS = storeFilePath.getFileSystem();
        } catch (IOException e) {
            Thrower.errAndThrow(
                "MAIN",
                String.format("INVALID BINLOG OFFSET STORE PATH: %s", offsetStoreFilePath)
            );
        }

        String offsetValue = "";

        try (FSDataInputStream storeInputStream = storeFS.open(storeFilePath)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(storeInputStream));

            StringBuilder content = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }

            offsetValue = content.toString();
        } catch (FileNotFoundException e) {
            LOG.info(">>> [MAIN] OFFSET STORE DOES NOT EXIST, SNAPSHOT + CDC");
        } catch (Exception e) {
            LOG.error(">>> [MAIN] OFFSET STORE ERROR");
            LOG.error(FlinkCDCMulti.offsetValue);
            throw e;
        }

        if (StringUtils.isNullOrWhitespaceOnly(offsetValue)) {
            LOG.info(">>> [MAIN] EMPTY OFFSET STORE, SNAP + CDC");
            return;
        }

        LOG.info(">>> [MAIN] USING OFFSET: {}", offsetValue);
        configJSON.put("offset.value", offsetValue);
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
            default:
                String msg = String.format("UNSUPPORTED SOURCE TYPE: %s", sourceType);
                LOG.error(">>> [MAIN] {}", msg);
                throw new RuntimeException(msg);
        }
    }

    private static void createOffsetStoreStream() {
        String offsetStorePath = configJSON.getString("offset.store.path");

        if (StringUtils.isNullOrWhitespaceOnly(offsetStorePath)) {
            return;
        }

        LOG.info(">>> [MAIN] CREATING OFFSET STREAM");

        sourceStream
            .keyBy(new NullByteKeySelector<>())
            .process(new TimestampOffsetStoreProcessFunction())
            .addSink(new SingleFileSinkFunction(new Path(offsetStorePath)))
            .setParallelism(1);
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

    private static void configureCheckpoint() {
        int checkpointInterval = configJSON.getIntValue("checkpoint.interval") > 0 ? configJSON.getIntValue("checkpoint.interval") : 30;
        String checkpointStorage = Objects.requireNonNullElse(configJSON.getString("checkpoint.storage"), "jobmanager");
        String checkpointDirectory;

        if (checkpointStorage.equals("jobmanager") || checkpointStorage.equals("filesystem")) {
            if (checkpointStorage.equals("filesystem")) {
                LOG.info(">>> [MAIN] LOADED CHECKPOINT STORAGE: {}", checkpointStorage);

                checkpointDirectory = configJSON.getString("checkpoint.directory");

                if (checkpointDirectory == null) {
                    LOG.error(">>> [MAIN] CHECKPOINT DIRECTORY IS EMPTY FOR FILESYSTEM STORAGE");
                    throw new RuntimeException();
                }

                flinkConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, checkpointStorage);
                flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDirectory);
            }
        } else {
            LOG.error(">>> [MAIN] CHECKPOINT STORAGE NOT IN FORMAT: filesystem | jobmanager");
            throw new RuntimeException();
        }

        env.configure(flinkConfig);
        env.enableCheckpointing(checkpointInterval * 1000L);
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

        sourceType = configJSON.getString("source.type");
    }

    private static void configureTableNameMap() {
        tableNameMap = configJSON.getJSONObject("table.name.map");

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
