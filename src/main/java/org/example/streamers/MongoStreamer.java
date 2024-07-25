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
import org.example.deserializers.MongoDebeziumToJSONDeserializer;
import org.example.processfunctions.mongodb.DelayedStopSignalProcessFunction;
import org.example.processfunctions.mongodb.SideInputProcessFunction;
import org.example.utils.*;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MongoStreamer implements Streamer<String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private final String hosts;
    private final String databaseName;
    private final String collectionFullName;
    private final String collectionName;
    private final String username;
    private final String password;
    private final String offsetValue;
    private final boolean snapshotOnly;
    private Map<String, Tuple2<OutputTag<String>, Schema>> tagSchemaMap;
    private String mongodbDeserializationMode;
    private Map<String, Tuple2<OutputTag<String>, String>> tagSchemaStringMap;

    public MongoStreamer(JSONObject configJSON) {
        // TODO: SUPPORT DB LEVEL CDC FOR MONGODB v4+
        LOG.warn(">>> [MONGO-STREAMER] CURRENTLY ONLY SINGLE DB AND COLLECTION IS SUPPORTED");
        LOG.warn(">>> [MONGO-STREAMER] BECAUSE THIS TOOL IS DEVELOPED AGAINST MONGO V3.6");

        this.hosts = Validator.ensureNotEmpty("source.hosts", configJSON.getString("source.hosts"));
        this.databaseName = Validator.ensureNotEmpty("source.database.name", configJSON.getString("source.database.name"));
        this.collectionFullName = Validator.ensureNotEmpty("source.collection.name", configJSON.getString("source.collection.name"));
        this.username = configJSON.getString("source.username");
        this.password = configJSON.getString("source.password");
        this.offsetValue = configJSON.getString("offset.value");
        this.mongodbDeserializationMode = configJSON.getString("mongodb.deserialization.mode");

        if (this.databaseName.matches("(?i)^(?:admin|config|local)$")) {
            Thrower.errAndThrow(
                "MONGO-STREAMER",
                String.format("FLINK CDC CANNOT STREAM FROM SYSTEM DB: %s", this.databaseName)
            );
        }

        if (!this.collectionFullName.contains(".")) {
            Thrower.errAndThrow(
                "MONGO-STREAMER",
                String.format("COLLECTION NAME MUST BE PREFIXED WITH DB (DB.COLLECTION): %s", this.collectionFullName)
            );
        }

        if (StringUtils.isNullOrWhitespaceOnly(mongodbDeserializationMode)) {
            LOG.warn(">>> [MONGO-STREAMER] MONGODB DESERIALIZATION MODE NOT SET, DEFAULT TO: top-level-type");
            mongodbDeserializationMode = "top-level-type";
        } else {
            switch (mongodbDeserializationMode) {
                case "doc-string":
                case "top-level-string":
                case "top-level-type":
                    break;
                default:
                    Thrower.errAndThrow(
                        "MONGO-STREAMER",
                        String.format("UNKNOWN MONGODB DESERIALIZATION MODE: %s", mongodbDeserializationMode)
                    );
            }

            LOG.info(">>> [MONGO-STREAMER] MONGODB DESERIALIZATION MODE: {}", mongodbDeserializationMode);
        }


        this.collectionName = this.collectionFullName.split("\\.")[1];

        if (StringUtils.isNullOrWhitespaceOnly(this.username)
            || StringUtils.isNullOrWhitespaceOnly(this.password)) {
            LOG.warn(">>> [MONGO-STREAMER] NOT USING AUTHENTICATION");
        }

        this.snapshotOnly = Boolean.parseBoolean(configJSON.getString("snapshot.only"));

        if (snapshotOnly) {
            LOG.info(">>> [MONGO-STREAMER] SNAPSHOT ONLY MODE");
        } else {
            LOG.info(">>> [MONGO-STREAMER] SNAPSHOT + CDC MODE");
        }
    }

    public MongoDBSource<String> getSource() {
        StartupOptions startupOptions;

        if (StringUtils.isNullOrWhitespaceOnly(offsetValue)) {
            startupOptions = StartupOptions.initial();
        } else {
            if (!offsetValue.matches("^[1-9][0-9]*$")) {
                Thrower.errAndThrow("MONGO-STREAMER", String.format("OFFSET NOT IN TIMESTAMP MILLISECONDS FORMAT: %s", offsetValue));
            }

            startupOptions = StartupOptions.timestamp(Long.parseLong(offsetValue));
        }

        return MongoDBSource.<String>builder()
            .hosts(hosts)
            .scheme("mongodb")
            .username(username)
            .password(password)
            .databaseList(databaseName)
            .collectionList(collectionFullName)
            .deserializer(new MongoDebeziumToJSONDeserializer(mongodbDeserializationMode, tagSchemaMap))
            .startupOptions(startupOptions)
            .build();
    }

    public Map<String, Tuple2<OutputTag<String>, Schema>> createTagSchemaMap() {
        LOG.info(">>> [MONGO-STREAMER] CREATING TAG SCHEMA MAP");

        // DB.TABLE -> (OUTPUT-TAG, SCHEMA)
        Map<String, Tuple2<OutputTag<String>, Schema>> tagSchemaMap = new HashMap<>();

        // Connect to MongoDB
        try (MongoClient mongoClient = MongoClients.create(String.format("mongodb://%s/%s", hosts, databaseName))) {
            final MongoDatabase db = mongoClient.getDatabase(databaseName);
            Document buildInfo = db.runCommand(new Document("buildInfo", 1));
            String version = buildInfo.getString("version");

            LOG.info(">>> [MONGO-STREAMER] MONGODB VERSION: {}", version);

            if (VersionUtil.compareVersions(version, "4.0.0") < 0) {
                /*
                * NOTE:
                * Even though we can change the source code to use `resumeAfter` token, it
                * cannot be used for source splitting because splitting is timestamp
                * based. This is too much work and diverges future version updates. This is
                * purely required for working with MongoDB v3.6 which dates back many years.
                * So the decision is to drop this support.
                * */
                LOG.warn(">>> [MONGO-STREAMER] MONGODB VERSION < 4.0, EITHER SNAPSHOT OR CDC FROM LATEST OFFSET");
                LOG.warn(">>> [MONGO-STREAMER] TIMESTAMP OFFSET IS SILENTLY IGNORED");
                LOG.warn(">>> [MONGO-STREAMER] CAN ONLY HAVE CURRENCY = 1 AS TIMESTAMP SPLITTING WILL NOT WORK");
            }

            final String sanitizedDatabaseName = Sanitizer.sanitize(databaseName);
            final String sanitizedCollectionName = Sanitizer.sanitize(collectionName);

            LOG.info(
                ">>> [MONGO-STREAMER] FETCHING SCHEMA FOR {}.{}",
                sanitizedDatabaseName, sanitizedCollectionName
            );

            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            Map<String, Class<?>> fieldTypes = new NoOverwriteHashMap<>();

            if ("doc-string".equals(mongodbDeserializationMode)) {
                fieldTypes.put("doc", String.class);
            } else {
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

                            if ("top-level-type".equals(mongodbDeserializationMode)) {
                                if (fieldTypes.containsKey(fieldName)) {
                                    if (fieldTypes.get(fieldName) != value.getClass()) {
                                        LOG.error(">>> [MONGO-STEAMER] FIELD TYPE CONFLICT: {} ({} <-> {})", fieldName, fieldTypes.get(fieldName), value.getClass());
                                        Thrower.errAndThrow("MONGO-STREAMER",
                                            ">>> [MONGO-STREAMER] CONFLICTING SCHEMA FOUND IN DOC SAMPLES, MUST CHANGE MODE TO: top-level-string"
                                        );
                                    }
                                } else {
                                    fieldTypes.put(fieldName, value != null ? value.getClass() : Object.class);
                                }
                            } else if ("top-level-string".equals(mongodbDeserializationMode)) {
                                if (!fieldTypes.containsKey(fieldName)) {
                                    fieldTypes.put(fieldName, String.class);
                                }
                            }
                        }
                    }
                }

                LOG.info(">>> [MONGO-STREAMER] FETCHED {} SAMPLES", count);

                if (count < 1) {
                    Thrower.errAndThrow("MONGO-STREAMER", "CANNOT INFER SCHEMA FROM EMPTY COLLECTION");
                } else if (count < 50) {
                    LOG.warn(">>> [MONGO-STREAMER] USING ONLY {} SAMPLES TO INFER SCHEMA, MAY NOT BE ACCURATE", count);
                }
            }

            FieldAssembler<Schema> fieldAssembler = AVROUtils.createFieldAssemblerWithFieldTypes(fieldTypes);

            AVROUtils.addFieldToFieldAssembler(fieldAssembler, "_op", String.class, false);
            AVROUtils.addFieldToFieldAssembler(fieldAssembler, "_ts", Long.class, false);

            final Schema avroSchema = fieldAssembler.endRecord();

            // TODO: CUSTOM OUTPUT PATH FORMAT
            final String outputTagID = String.format("%s__%s", sanitizedDatabaseName, sanitizedCollectionName);
            final OutputTag<String> outputTag = new OutputTag<>(outputTagID) {};

            tagSchemaMap.put(sanitizedCollectionName, Tuple2.of(outputTag, avroSchema));

            LOG.debug(">>> [MONGO-STREAMER] AVRO SCHEMA INFERRED FROM 100 SAMPLES");
            LOG.debug(avroSchema.toString(true));
        }

        this.tagSchemaMap = tagSchemaMap;
        this.tagSchemaStringMap = tagSchemaMap.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> Tuple2.of(entry.getValue().f0, entry.getValue().f1.toString())
            ));

        return tagSchemaMap;
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
