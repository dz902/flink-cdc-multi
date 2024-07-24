package org.example;

public class DataStreamJob {
//    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
//
//    public static void main(String[] args) throws Exception {
//        // FLINK ENV SETUP
//
//        LOG.info(">>> [MAIN] SETTING UP FLINK ENV");
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        Configuration flinkConfig = GlobalConfiguration.loadConfiguration();
//
//        // YOU MUST MANUALLY LOAD CONFIG FOR S3 REGION TO TAKE EFFECT
//        PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(flinkConfig);
//        FileSystem.initialize(flinkConfig, pluginManager);
//
//        // <<<
//
//        // CLI OPTIONS >>>
//        LOG.info(">>> [MAIN] ARGS: {}", Arrays.toString(args));
//
//        Options options = new Options();
//        options.addOption("c", "config", true, "config json");
//        options.addOption(null, "debug", false, "Enable debug logging.");
//
//        CommandLineParser parser = new DefaultParser();
//        CommandLine cmd = parser.parse(options, args);
//
//        if (cmd.hasOption("debug")) {
//            LOG.info(">>> [MAIN] DEBUG MODE");
//
//            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
//            org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
//
//            LoggerConfig loggerConfig = new LoggerConfig("flink-cdc-multi", Level.TRACE, true);
//            config.addLogger("flink-cdc-multi", loggerConfig);
//
//            ctx.updateLoggers();
//        }
//
//        final String argConfig = cmd.getOptionValue("c");
//
//        // TODO: CREDENTIALS FROM AWS SECRETS MANAGER
//
//        JSONObject tableNameMap = null;
//        JSONObject binlogOffset = null;
//        String binlogOffsetStorePath = null;
//        String binlogOffsetStoreFilePath = null;
//        String hostname = "localhost";
//        int port = 3306;
//        String username = "test";
//        String password = "";
//        String sinkPath = "";
//        String sourceId = "";
//        String databaseName = "";
//        String timezone = "UTC";
//        String checkpointStorage;
//        String checkpointDirectory;
//        int checkpointInterval = 30;
//        Configuration checkpointConfig = new Configuration();
//
//        // TODO: ADD STATS TABLE
//
//        if (argConfig != null) {
//            LOG.info(">>> [MAIN] LOADING CONFIG FROM {}", argConfig);
//
//            Path configPath = new Path(argConfig);
//
//            FileSystem configFS;
//            try {
//                configFS = configPath.getFileSystem();
//            } catch (IOException e) {
//                LOG.error(">>> [MAIN] INVALID CONFIG PATH: {}", argConfig);
//                throw e;
//            }
//
//            String configJSONString = "";
//
//            try (FSDataInputStream configInputStream = configFS.open(configPath)) {
//                BufferedReader reader = new BufferedReader(new InputStreamReader(configInputStream));
//
//                StringBuilder content = new StringBuilder();
//                String line;
//                while ((line = reader.readLine()) != null) {
//                    content.append(line).append("\n");
//                }
//
//                configJSONString = content.toString();
//            } catch (Exception e) {
//                LOG.error(">>> [MAIN] CONFIG FILE ERROR");
//                LOG.error(configJSONString);
//                throw e;
//            }
//
//            JSONObject configJSON;
//            try {
//                configJSON = JSONObject.parseObject(configJSONString);
//            } catch (Exception e) {
//                // do not print json contents as it may contain credentials
//                LOG.error(">>> [MAIN] CONFIG JSON IS NOT VALID");
//                throw new RuntimeException();
//            }
//
//            tableNameMap = configJSON.getJSONObject("table.name.map");
//
//            if (tableNameMap != null) {
//                LOG.info(">>> [MAIN] LOADED TABLE NAME MAP: {}", tableNameMap);
//            }
//
//            binlogOffsetStorePath = configJSON.getString("binlog.offset.store.path");
//            binlogOffset = configJSON.getJSONObject("binlog.offset");
//            sinkPath = configJSON.getString("sink.path");
//            sourceId = configJSON.getString("source.id");
//            hostname = Objects.requireNonNullElse(configJSON.getString("source.hostname"), hostname);
//            port = configJSON.getIntValue("source.port") > 0 ? configJSON.getIntValue("source.port") : port;
//            username = Objects.requireNonNullElse(configJSON.getString("source.username"), username);
//            password = Objects.requireNonNullElse(configJSON.getString("source.password"), password);
//            databaseName = configJSON.getString("source.database.name");
//            timezone = Objects.requireNonNullElse(configJSON.getString("source.timezone"), timezone);
//            checkpointInterval = configJSON.getIntValue("checkpoint.interval") > 0 ? configJSON.getIntValue("checkpoint.interval") : checkpointInterval;
//            checkpointStorage = Objects.requireNonNullElse(configJSON.getString("checkpoint.storage"), "jobmanager");
//
//            if (checkpointStorage.equals("jobmanager") || checkpointStorage.equals("filesystem")) {
//                if (checkpointStorage.equals("filesystem")) {
//                    LOG.info(">>> [MAIN] LOADED CHECKPOINT STORAGE: {}", checkpointStorage);
//
//                    checkpointDirectory = configJSON.getString("checkpoint.directory");
//
//                    if (checkpointDirectory == null) {
//                        LOG.error(">>> [MAIN] CHECKPOINT DIRECTORY IS EMPTY FOR FILESYSTEM STORAGE");
//                        throw new RuntimeException();
//                    }
//
//                    flinkConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, checkpointStorage);
//                    flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDirectory);
//                }
//            } else {
//                LOG.error(">>> [MAIN] CHECKPOINT STORAGE NOT IN FORMAT: filesystem | jobmanager");
//                throw new RuntimeException();
//            }
//
//            LOG.info(">>> [MAIN] LOADED SINK PATH: {}", sinkPath);
//            LOG.info(">>> [MAIN] LOADED SOURCE: {}:{}", hostname, port);
//        } else {
//            LOG.warn(">>> [MAIN] NO CONFIG PROVIDED");
//        }
//
//        String sanitizedDatabaseName = databaseName.replace('-', '_');
//        if (!databaseName.equals(sanitizedDatabaseName)) {
//            LOG.warn(">>> [MAIN] DATABASE NAME IS SANITIZED: {} -> {}", databaseName, sanitizedDatabaseName);
//        }
//
//        env.configure(flinkConfig);
//        env.enableCheckpointing(checkpointInterval * 1000L);
//
//        // BINLOG OFFSET
//
//        StartupOptions startupOptions = StartupOptions.initial();
//
//        if (binlogOffsetStorePath != null && !binlogOffsetStorePath.isBlank()) {
//            if (binlogOffset != null) {
//                String msg = "BINLOG STORE AND EXPLICIT BINLOG OFFSET CANNOT BE USED TOGETHER";
//                LOG.error(">>> [MAIN] {}", msg);
//                throw new RuntimeException(msg);
//            }
//
//
//            // TODO: MANUAL BINLOG OFFSET ID OVERRIDE, ONE SOURCE MAY HAVE MULTIPLE JOBS
//            binlogOffsetStoreFilePath = binlogOffsetStorePath + String.format("/%s_offset.txt", sourceId);
//
//            LOG.info(">>> [MAIN] LOADING BINLOG OFFSET FROM STORE: {}", binlogOffsetStoreFilePath);
//
//            Path storeFilePath = new Path(binlogOffsetStoreFilePath);
//
//            FileSystem storeFS;
//            try {
//                storeFS = storeFilePath.getFileSystem();
//            } catch (IOException e) {
//                LOG.error(">>> [MAIN] INVALID BINLOG OFFSET STORE PATH: {}", binlogOffsetStorePath);
//                throw e;
//            }
//
//            String storeString = "";
//
//            try (FSDataInputStream storeInputStream = storeFS.open(storeFilePath)) {
//                BufferedReader reader = new BufferedReader(new InputStreamReader(storeInputStream));
//
//                StringBuilder content = new StringBuilder();
//                String line;
//                while ((line = reader.readLine()) != null) {
//                    content.append(line).append("\n");
//                }
//
//                storeString = content.toString();
//
//                String[] storeStringSplits = storeString.trim().split(",");
//                String binlogOffsetFile = storeStringSplits[0];
//                long binlogOffsetPos = Long.parseLong(storeStringSplits[1]);
//
//                if (binlogOffsetFile != null && binlogOffsetPos > 0) {
//                    LOG.info(">>> [MAIN] BINLOG OFFSET LOADED: {},{}", binlogOffsetFile, binlogOffsetPos);
//                    startupOptions = StartupOptions.specificOffset(binlogOffsetFile, binlogOffsetPos);
//                } else {
//                    String msg = "BINLOG OFFSET NOT IN FORMAT: file,pos";
//                    LOG.error(">>> [MAIN] {}", msg);
//                    throw new RuntimeException(msg);
//                }
//            } catch (FileNotFoundException e) {
//                LOG.info(">>> [MAIN] BINLOG OFFSET STORE DOES NOT EXIST, SNAPSHOT + CDC");
//            } catch (Exception e) {
//                LOG.error(">>> [MAIN] BINLOG OFFSET STORE ERROR");
//                LOG.error(storeString);
//                throw e;
//            }
//        } else if (binlogOffset != null) {
//            String binlogOffsetFile = binlogOffset.getString("file");
//            long binlogOffsetPos = binlogOffset.getLongValue("pos");
//
//            if (binlogOffsetFile != null && binlogOffsetPos > 0) {
//                startupOptions = StartupOptions.specificOffset(binlogOffsetFile, binlogOffsetPos);
//            } else {
//                LOG.error(">>> [MAIN] BINLOG OFFSET NOT IN FORMAT: { \"file\": string, \"pos\": int }");
//                throw new RuntimeException();
//            }
//
//            LOG.info(">>> [MAIN] BINLOG OFFSET = {}, {}", binlogOffsetFile, binlogOffsetPos);
//        } else {
//            LOG.info(">>> [MAIN] NO BINLOG OFFSET, SNAPSHOT + CDC");
//        }
//
//        // <<<
//
//        // BASIC BEST PRACTICE DEBEZIUM CONFIGURATIONS
//
//        Properties debeziumProperties = new Properties();
//        debeziumProperties.setProperty("bigint.unsigned.handling.mode","long");
//        debeziumProperties.setProperty("decimal.handling.mode","string");
//        debeziumProperties.setProperty("database.history.skip.unparseable.ddl", "false");
//
//        // <<<
//
//        // CREATE FLINK CDC SOURCE
//
//        LOG.info(">>> [MAIN] CREATING FLINK-CDC SOURCE");
//
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//            .hostname(hostname)
//            .port(port)
//            .databaseList(databaseName)
//            .tableList(databaseName + ".*")
//            .username(username)
//            .password(password)
//            .serverTimeZone(timezone)
//            .scanNewlyAddedTableEnabled(true)
//            .startupOptions(startupOptions)
//            .deserializer(new MySQLDebeziumToJSONDeserializer())
//            .includeSchemaChanges(true)
//            .debeziumProperties(debeziumProperties)
//            .build();
//
//        // <<<
//
//        // <<<
//
//        // CREATE FLINK SOURCE FROM FLINK CDC SOURCE
//
//        LOG.info(">>> [MAIN] CREATING STREAM FROM FLINK-CDC SOURCE");
//
//        DataStream<String> source = env
//                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
//                .setParallelism(1);
//
//        // MAP TABLE NAME -> OUTPUT TAG FOR SIDE OUTPUT
//
//        LOG.info(">>> [MAIN] CREATING TABLE MAPPING");
//
//        Map<String, Tuple2<OutputTag<String>, Schema>> tableTagSchemaMap = new HashMap<>();
//        try (Connection connection = DriverManager.getConnection(String.format("jdbc:mysql://%s:%d?tinyInt1isBit=false", hostname, port), username, password)) {
//            DatabaseMetaData metaData = connection.getMetaData();
//            ResultSet tables = metaData.getTables(databaseName, null, "%", new String[]{"TABLE"});
//            while (tables.next()) {
//                String tableName = tables
//                    .getString(3);
//                String sanitizedTableName = tableName
//                    .replace('-', '_');
//                if (!tableName.equals(sanitizedTableName)) {
//                    LOG.warn(">>> [MAIN] TABLE NAME IS SANITIZED: {} -> {}", tableName, sanitizedTableName);
//                }
//
//                String mappedTableName;
//                String sanitizedMappedTableName = sanitizedTableName;
//                if (tableNameMap != null) {
//                    mappedTableName = tableNameMap.getString(tableName);
//                    if (mappedTableName != null) {
//                        sanitizedMappedTableName = mappedTableName.replace('-', '_');
//                    }
//                }
//
//                // TODO: MULTIPLE DB?
//
//                ResultSet columns = metaData.getColumns(databaseName, null, tableName, "%");
//
//                SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(sanitizedTableName).fields();
//                while (columns.next()) {
//                    String columnName = columns
//                        .getString("COLUMN_NAME");
//                    String sanitizedColumnName = columnName
//                        .replace('-', '_');
//                    if (!columnName.equals(sanitizedColumnName)) {
//                        LOG.warn(
//                            ">>> [MAIN] COLUMN NAME SANITIZED: ({}) {} -> {}",
//                            sanitizedTableName,
//                            columnName,
//                            sanitizedColumnName
//                        );
//                    }
//
//
//                    String columnType = columns.getString("TYPE_NAME");
//
//                    // NOTE: NULL is always allowed
//                    LOG.debug(">>> [MAIN] CONVERTING COLUMN: {}.{}: {}", sanitizedTableName, sanitizedColumnName, columnType);
//
//                    fieldAssembler = addNullableFieldToSchema(fieldAssembler, sanitizedColumnName, columnType);
//                }
//
//                fieldAssembler = addFieldToSchema(fieldAssembler, "_op", "VARCHAR");
//                fieldAssembler = addFieldToSchema(fieldAssembler, "_ts", "BIGINT");
//                Schema avroSchema = fieldAssembler.endRecord();
//
//                final String outputTagID = String.format("%s__%s", sanitizedDatabaseName, sanitizedMappedTableName);
//                final OutputTag<String> outputTag = new OutputTag<>(outputTagID) {};
//                tableTagSchemaMap.put(sanitizedTableName, Tuple2.of(outputTag, avroSchema));
//
//                LOG.info(
//                    ">>> [MAIN] TABLE-TAG-SCHEMA MAP FOR: {}{}", String.format("%s.%s", sanitizedDatabaseName, sanitizedTableName) ,
//                        (
//                            !sanitizedTableName.equals(sanitizedMappedTableName) ? ("(" + sanitizedMappedTableName + ")") : ""
//                        )
//                );
//                LOG.info(String.valueOf(avroSchema));
//            }
//        } catch (SQLException e) {
//            LOG.error(">>> [MAIN] UNABLE TO CONNECT TO SOURCE, EXCEPTION:");
//            throw e;
//        }
//
//        // <<<
//
//        // >>> CAPTURE DDL STATEMENTS TO SPECIAL DDL TABLE
//
//        final String sanitizedDDLTableName = String.format("_%s_ddl", sanitizedDatabaseName);
//        SchemaBuilder.FieldAssembler<Schema> ddlFieldAssembler = SchemaBuilder.record(sanitizedDDLTableName).fields();
//
//        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_ddl", "VARCHAR");
//        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_ddl_tbl", "VARCHAR");
//        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_ts", "BIGINT");
//        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_binlog_file", "VARCHAR");
//        ddlFieldAssembler = addFieldToSchema(ddlFieldAssembler, "_binlog_pos_end", "BIGINT");
//        Schema ddlAvroSchema = ddlFieldAssembler.endRecord();
//
//        final String outputTagID = String.format("%s__%s", sanitizedDatabaseName, sanitizedDDLTableName);
//        final OutputTag<String> ddlOutputTag = new OutputTag<>(outputTagID) {};
//        tableTagSchemaMap.put(sanitizedDDLTableName, Tuple2.of(ddlOutputTag, ddlAvroSchema));
//
//        LOG.info(
//            ">>> [MAIN] TABLE-TAG-SCHEMA MAP FOR: {}", String.format("%s.%s", sanitizedDatabaseName, sanitizedDDLTableName)
//        );
//        LOG.info(String.valueOf(ddlAvroSchema));
//
//        // CHECK FOR DDL STOP SIGNAL
//        // NOTE: MUST BE KEYED STREAM TO USE TIMER
//
//        SingleOutputStreamOperator<String> mainDataStream = source
//            .keyBy(new NullByteKeySelector<>())
//            .process(new DelayedStopSignalProcessFunction())
//            .setParallelism(1)
//            .keyBy(new NullByteKeySelector<>())
//            .process(new StopSignalCheckerProcessFunction(tableTagSchemaMap))
//            .setParallelism(1);
//
//        // <<<
//
//        // CREATE SIDE STREAMS
//
//        for (Map.Entry<String, Tuple2<OutputTag<String>, Schema>> entry : tableTagSchemaMap.entrySet()) {
//            OutputTag<String> outputTag = entry.getValue().f0;
//            Schema avroSchema = entry.getValue().f1;
//
//            SingleOutputStreamOperator<GenericRecord> sideOutputStream = mainDataStream
//                .getSideOutput(outputTag)
//                .map(new JSONToGenericRecordMapFunction(avroSchema))
//                .returns(new GenericRecordAvroTypeInfo(avroSchema));
//
//            ParquetWriterFactory<GenericRecord> compressedParquetWriterFactory = new ParquetWriterFactory<>(
//                out -> AvroParquetWriter.<GenericRecord>builder(out).withSchema(avroSchema)
//                    .withDataModel(GenericData.get())
//                    .withCompressionCodec(CompressionCodecName.SNAPPY)
//                    .build()
//            );
//
//            Path outputPath = new Path(
//                sinkPath + "/" +
//                    (!sourceId.isBlank() ? String.format("%s_", sourceId) : "") +
//                    outputTag.getId());
//            FileSink<GenericRecord> sink = FileSink
//                .forBulkFormat(outputPath, compressedParquetWriterFactory)
//                .withRollingPolicy(
//                    OnCheckpointRollingPolicy
//                        .build()
//                )
//                .withBucketAssigner(new DatabaseTableDateBucketAssigner())
//                .build();
//
//            sideOutputStream.sinkTo(sink).setParallelism(1);
//        }
//
//        // <<<
//
//
//        if (binlogOffsetStoreFilePath != null && !binlogOffsetStoreFilePath.isBlank()) {
//            LOG.info(">>> [MAIN] CREATING BINLOG OFFSET STORE SINK");
//
//            Path path = new Path(binlogOffsetStoreFilePath);
//            FileSink<String> sink = FileSink
//                .forRowFormat(path, new SimpleStringEncoder<String>("UTF-8"))
//                .withRollingPolicy(
//                    OnCheckpointRollingPolicy
//                        .build()
//                )
//                .build();
//
//            mainDataStream
//                .keyBy(new NullByteKeySelector<>())
//                .process(new BinlogOffsetStoreProcessFunction())
//                .addSink(new SingleFileSinkFunction(new Path(binlogOffsetStoreFilePath)))
//                .setParallelism(1);
//        }
//
//        // PRINT FROM MAIN STREAMS
//        // TODO: DRYRUN MODE
//
//        mainDataStream
//            .print("MAIN ")
//            .setParallelism(1);
//
//        // <<<
//
//        // NO RESTART TO STOP AT STOP SIGN
//
//        env.setRestartStrategy(RestartStrategies.noRestart());
//
//        // <<<
//
//        env.execute("Print MySQL Snapshot + Binlog");
//    }
//
//    private static SchemaBuilder.FieldAssembler<Schema> addNullableFieldToSchema(
//        SchemaBuilder.FieldAssembler<Schema> fieldAssembler,
//        String columnName, String columnType
//    ) {
//        return addFieldToSchema(fieldAssembler, columnName, columnType, true);
//    }
//
//    private static SchemaBuilder.FieldAssembler<Schema> addFieldToSchema(
//        SchemaBuilder.FieldAssembler<Schema> fieldAssembler,
//        String columnName, String columnType
//    ) {
//        return addFieldToSchema(fieldAssembler, columnName, columnType, false);
//    }
//
//    private static SchemaBuilder.FieldAssembler<Schema> addFieldToSchema(
//        SchemaBuilder.FieldAssembler<Schema> fieldAssembler,
//        String columnName, String columnType, boolean isNullable
//    ) {
//        // REMOVE "UNSIGNED" OR OTHER NASTY THINGS
//        columnType = columnType.replaceFirst(" .*", "");
//
//        switch (columnType.toUpperCase()) {
//            case "INT":
//            case "TINYINT":
//            case "SMALLINT":
//            case "MEDIUMINT":
//            case "DATE":
//                fieldAssembler = isNullable
//                    ? fieldAssembler.name(columnName).type().unionOf().nullType().and().intType().endUnion().nullDefault()
//                    : fieldAssembler.name(columnName).type().intType().noDefault();
//                break;
//            case "BIGINT":
//            case "DATETIME":
//            case "TIME":
//                fieldAssembler = isNullable
//                    ? fieldAssembler.name(columnName).type().unionOf().nullType().and().longType().endUnion().nullDefault()
//                    : fieldAssembler.name(columnName).type().longType().noDefault();
//                break;
//            case "FLOAT":
//            case "DOUBLE":
//                fieldAssembler = isNullable
//                    ? fieldAssembler.name(columnName).type().unionOf().nullType().and().doubleType().endUnion().nullDefault()
//                    : fieldAssembler.name(columnName).type().doubleType().noDefault();
//                break;
//            case "BIT":
//            case "BOOL":
//            case "BOOLEAN":
//                fieldAssembler = isNullable
//                    ? fieldAssembler.name(columnName).type().unionOf().nullType().and().booleanType().endUnion().nullDefault()
//                    : fieldAssembler.name(columnName).type().booleanType().noDefault();
//                break;
//            case "VARCHAR":
//            case "CHAR":
//            case "TEXT":
//            case "DECIMAL":
//            case "TIMESTAMP":
//            default:
//                fieldAssembler = isNullable
//                    ? fieldAssembler.name(columnName).type().unionOf().nullType().and().stringType().endUnion().nullDefault()
//                    : fieldAssembler.name(columnName).type().stringType().noDefault();
//                break;
//        }
//
//        return fieldAssembler;
//    }
//
}
