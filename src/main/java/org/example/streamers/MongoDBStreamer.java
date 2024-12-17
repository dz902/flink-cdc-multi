package org.example.streamers;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.*;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.util.VersionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.example.deserializers.MongoDBDebeziumToJSONDeserializer;
import org.example.processfunctions.mongodb.DelayedStopSignalProcessFunction;
import org.example.processfunctions.mongodb.SideInputProcessFunction;
import org.example.utils.*;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MongoDBStreamer implements Streamer<String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private final String hosts;
    private final String databaseName;
    private final String collectionFullName;
    private final String collectionName;
    private final String username;
    private final String password;
    private final String offsetValue;
    private final boolean snapshotOnly;
    private final String connectionOptions;
    private final JSONObject collNameMap;
    private String startupMode;
    private String mongoDBDeserializationMode;
    private Map<String, Tuple2<OutputTag<String>, String>> tagSchemaStringMap;
    private boolean compatibilityMode = false;

    public MongoDBStreamer(JSONObject configJSON) {
        this.hosts = Validator.ensureNotEmpty("source.hosts", configJSON.getString("source.hosts"));
        this.databaseName = Validator.ensureNotEmpty("source.database.name", configJSON.getString("source.database.name"));
        this.collectionFullName = Validator.ensureNotEmpty("source.collection.name", configJSON.getString("source.collection.name"));
        this.username = configJSON.getString("source.username");
        this.password = configJSON.getString("source.password");
        this.offsetValue = configJSON.getString("offset.value");
        this.mongoDBDeserializationMode = configJSON.getString("mongodb.deserialization.mode");

        if (StringUtils.isNullOrWhitespaceOnly(this.username) || StringUtils.isNullOrWhitespaceOnly(this.password)) {
            LOG.warn(">>> [MONGODB-STREAMER] NOT USING AUTHENTICATION");
        } else {
            LOG.info(">>> [MONGODB-STREAMER] USING AUTHENTICATION");
        }

        if (this.databaseName.matches("(?i)^(?:admin|config|local)$")) {
            Thrower.errAndThrow(
                "MONGODB-STREAMER",
                String.format("FLINK CDC CANNOT STREAM FROM SYSTEM DB: %s", this.databaseName)
            );
        }

        if (!this.collectionFullName.contains(".")) {
            Thrower.errAndThrow(
                "MONGODB-STREAMER",
                String.format("COLLECTION NAME MUST BE PREFIXED WITH DB (DB.COLLECTION): %s", this.collectionFullName)
            );
        }

        if (StringUtils.isNullOrWhitespaceOnly(mongoDBDeserializationMode)) {
            LOG.warn(">>> [MONGODB-STREAMER] MONGODB DESERIALIZATION MODE NOT SET, DEFAULT TO: top-level-type");
            mongoDBDeserializationMode = "top-level-type";
        } else {
            switch (mongoDBDeserializationMode) {
                case "doc-string":
                case "top-level-string":
                case "top-level-type":
                    break;
                default:
                    Thrower.errAndThrow(
                        "MONGODB-STREAMER",
                        String.format("UNKNOWN MONGODB DESERIALIZATION MODE: %s", mongoDBDeserializationMode)
                    );
            }

            LOG.info(">>> [MONGODB-STREAMER] MONGODB DESERIALIZATION MODE: {}", mongoDBDeserializationMode);
        }

        this.collectionName = this.collectionFullName.split("\\.")[1];

        this.collNameMap = configJSON.getJSONObject("collection.name.map");

        if (StringUtils.isNullOrWhitespaceOnly(this.username)
            || StringUtils.isNullOrWhitespaceOnly(this.password)) {
            LOG.warn(">>> [MONGODB-STREAMER] NOT USING AUTHENTICATION");
        }

        this.startupMode = configJSON.getString("startup.mode");

        switch (startupMode) {
            case "initial":
            case "earliest":
            case "latest":
            case "offset":
                break;
            default:
                startupMode = "initial";
        }

        this.snapshotOnly = Boolean.parseBoolean(configJSON.getString("snapshot.only"));

        if (snapshotOnly) {
            LOG.info(">>> [MONGODB-STREAMER] SNAPSHOT ONLY MODE, STARTUP MODE CHANGED: {} -> initial", startupMode);
        }

        this.connectionOptions = Validator.withDefault(configJSON.getString("mongodb.connection.options"), "");

        if (!StringUtils.isNullOrWhitespaceOnly(connectionOptions)) {
            LOG.info(">>> [MONGO-STREAMER] CONNECTION OPTIONS: {}", connectionOptions);
        }
    }

    public MongoDBSource<String> getSource() {
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
                if (StringUtils.isNullOrWhitespaceOnly(offsetValue)) {
                    LOG.info(">>> [MONGODB-STREAMER] NO OFFSET PROVIDED, STARTUP MODE CHANGED: offset -> initial");
                    startupOptions = StartupOptions.initial();
                } else {
                    if (compatibilityMode) {
                        LOG.warn(">>> [MONGODB-STREAMER] COMPATIBILITY MODE, STARTUP MODE CHANGED: offset -> latest");
                        startupOptions = StartupOptions.latest();
                    } else {
                        if (!offsetValue.matches("^[1-9][0-9]*$")) {
                            Thrower.errAndThrow("MONGODB-STREAMER", String.format("OFFSET NOT IN TIMESTAMP MILLISECONDS FORMAT: %s", offsetValue));
                        }

                        long offsetTimestamp = Long.parseLong(offsetValue);

                        LOG.info(">>> [MONGODB-STREAMER] STARTING FROM {} ({})",
                            DateTimeUtils.toDateString(offsetTimestamp),
                            offsetTimestamp
                        );

                        startupOptions = StartupOptions.timestamp(Long.parseLong(offsetValue));
                    }
                }

                break;
            default:
                startupOptions = StartupOptions.initial();
        }

        return MongoDBSource.<String>builder()
            .hosts(hosts)
            .scheme("mongodb")
            .username(username)
            .password(password)
            .databaseList(databaseName)
            .collectionList(collectionFullName)
            .deserializer(new MongoDBDebeziumToJSONDeserializer(mongoDBDeserializationMode, tagSchemaStringMap))
            .startupOptions(startupOptions)
            .batchSize(64) // TODO: THIS IS TO REDUCE RISK OF OOM, BUT SHOULD BE CONFIGURABLE
            .pollMaxBatchSize(64)
            .connectionOptions(connectionOptions)
            .build();
    }

    public Map<String, Tuple2<OutputTag<String>, String>> createTagSchemaMap() {
        LOG.info(">>> [MONGODB-STREAMER] CREATING TAG SCHEMA MAP");

        // DB.TABLE -> (OUTPUT-TAG, SCHEMA)
        Map<String, Tuple2<OutputTag<String>, Schema>> tagSchemaMap = new HashMap<>();

        try (MongoClient mongoClient = MongoClients.create(String.format("mongodb://%s:%s@%s/?%s", username, password, hosts, connectionOptions))) {
            final MongoDatabase db = mongoClient.getDatabase(databaseName);
            Document buildInfo = db.runCommand(new Document("buildInfo", 1));
            String version = buildInfo.getString("version");

            LOG.info(">>> [MONGODB-STREAMER] MONGODB VERSION: {}", version);

            if (VersionUtil.compareVersions(version, "4.0.0") < 0) {
                /*
                * NOTE:
                * Even though we can change the source code to use `resumeAfter` token, it
                * cannot be used for source splitting because splitting is timestamp
                * based. This is too much work and diverges future version updates. This is
                * purely required for working with MongoDB v3.6 which dates back many years.
                * So the decision is to drop this support.
                * */
                LOG.warn(">>> [MONGODB-STREAMER] MONGODB VERSION < 4.0, EITHER SNAPSHOT OR CDC FROM LATEST OFFSET");
                LOG.warn(">>> [MONGODB-STREAMER] TIMESTAMP OFFSET IS SILENTLY IGNORED");
                LOG.warn(">>> [MONGODB-STREAMER] CAN ONLY HAVE CONCURRENCY = 1 AS TIMESTAMP SPLITTING WILL NOT WORK");

                compatibilityMode = true;
            }

            final String sanitizedDatabaseName = Sanitizer.sanitize(databaseName);
            final String sanitizedCollectionName = Sanitizer.sanitize(collectionName);

            LOG.info(
                ">>> [MONGODB-STREAMER] FETCHING SCHEMA FOR {}.{}",
                sanitizedDatabaseName, sanitizedCollectionName
            );

            Map<String, Class<?>> fieldTypes = new NoOverwriteHashMap<>();

            if (!"doc-string".equals(mongoDBDeserializationMode)) {
                MongoDatabase database = mongoClient.getDatabase(databaseName);
                MongoCollection<Document> collection = database.getCollection(collectionName);

                // Sample size
                int sampleSize = 100;

                // Set to store field names

                int count = 0;
                try (MongoCursor<Document> cursor = collection.find().limit(sampleSize).iterator()) {
                    while (cursor.hasNext()) {
                        count += 1;
                        Document doc = cursor.next();
                        for (Map.Entry<String, Object> entry : doc.entrySet()) {
                            String fieldName = entry.getKey();
                            Object value = entry.getValue();

                            if ("top-level-type".equals(mongoDBDeserializationMode)) {
                                if (fieldTypes.containsKey(fieldName)) {
                                    if (fieldTypes.get(fieldName) != value.getClass()) {
                                        LOG.error(">>> [MONGO-STEAMER] FIELD TYPE CONFLICT: {} ({} <-> {})", fieldName, fieldTypes.get(fieldName), value.getClass());
                                        Thrower.errAndThrow("MONGODB-STREAMER",
                                            ">>> [MONGODB-STREAMER] CONFLICTING SCHEMA FOUND IN DOC SAMPLES, MUST CHANGE MODE TO: top-level-string"
                                        );
                                    }
                                } else {
                                    fieldTypes.put(fieldName, value != null ? value.getClass() : Object.class);
                                }
                            } else if ("top-level-string".equals(mongoDBDeserializationMode)) {
                                if (!fieldTypes.containsKey(fieldName)) {
                                    fieldTypes.put(fieldName, String.class);
                                }
                            }
                        }
                    }
                }

                LOG.info(">>> [MONGODB-STREAMER] FETCHED {} SAMPLES", count);

                if (count < 1) {
                    Thrower.errAndThrow("MONGODB-STREAMER", "CANNOT INFER SCHEMA FROM EMPTY COLLECTION");
                } else if (count < 50) {
                    LOG.warn(">>> [MONGODB-STREAMER] USING ONLY {} SAMPLES TO INFER SCHEMA, MAY NOT BE ACCURATE", count);
                }
            }

            FieldAssembler<Schema> fieldAssembler = AVROUtils.createFieldAssemblerWithFieldTypes(fieldTypes);

            // TODO: THIS DOES NOT LOOK LOOK, REFACTOR DOC-STRING LINE
            if ("doc-string".equals(mongoDBDeserializationMode)) {
                AVROUtils.addFieldToFieldAssembler(fieldAssembler, "_id", String.class, false);
                AVROUtils.addFieldToFieldAssembler(fieldAssembler, "doc", String.class, false);
            }

            AVROUtils.addFieldToFieldAssembler(fieldAssembler, "_op", String.class, false);
            AVROUtils.addFieldToFieldAssembler(fieldAssembler, "_ts", Long.class, false);

            final Schema avroSchema = fieldAssembler.endRecord();

            String mappedTableName;
            String sanitizedMappedCollName = sanitizedCollectionName;
            if (collNameMap != null) {
                mappedTableName = collNameMap.getString(collectionName);
                if (mappedTableName != null) {
                    sanitizedMappedCollName = Sanitizer.sanitize(mappedTableName);
                }
            }

            // TODO: CUSTOM OUTPUT PATH FORMAT
            final String outputTagID = String.format("%s__%s", sanitizedDatabaseName, sanitizedMappedCollName);
            final OutputTag<String> outputTag = new OutputTag<>(outputTagID) {};

            tagSchemaMap.put(sanitizedCollectionName, Tuple2.of(outputTag, avroSchema));

            LOG.info(
                ">>> [MAIN] TAG-SCHEMA MAP FOR: {}{}", String.format("%s.%s", sanitizedDatabaseName, sanitizedCollectionName) ,
                (
                    !sanitizedCollectionName.equals(sanitizedMappedCollName) ? ("(" + sanitizedMappedCollName + ")") : ""
                )
            );
            LOG.debug(">>> [MONGODB-STREAMER] AVRO SCHEMA INFERRED FROM 100 SAMPLES");
            LOG.debug(avroSchema.toString(true));
        }

        this.tagSchemaStringMap = tagSchemaMap.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> Tuple2.of(entry.getValue().f0, entry.getValue().f1.toString())
            ));

        return tagSchemaStringMap;
    }

    public SingleOutputStreamOperator<String> createMainDataStream(DataStream<String> sourceStream) {
        JSONObject snapshotConfig = new JSONObject();
        snapshotConfig.put("snapshot.only", snapshotOnly);

        return sourceStream
            .keyBy(new NullByteKeySelector<>())
            .process(new DelayedStopSignalProcessFunction(snapshotConfig))
            .keyBy(new NullByteKeySelector<>())
            .process(new SideInputProcessFunction(tagSchemaStringMap))
            .setParallelism(1);
    }
}
