package org.example.streamers;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.*;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.example.deserializers.MongoDebeziumToJSONDeserializer;
import org.example.utils.*;

import java.util.HashMap;
import java.util.Map;

public class MongoStreamer implements Streamer {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private final String hosts;
    private final String databaseName;
    private final String collectionFullName;
    private final String collectionName;
    private final String username;
    private final String password;
    private final String offsetValue;
    private Map<String, Tuple2<OutputTag<String>, Schema>> tagSchemaMap;
    private String mongodbDeserializationMode;

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

            FieldAssembler<Schema> fieldAssembler = AvroUtils.createFieldAssemblerWithFieldTypes(fieldTypes);

            AvroUtils.addFieldToFieldAssembler(fieldAssembler, "_op", String.class, false);
            AvroUtils.addFieldToFieldAssembler(fieldAssembler, "_ts", Long.class, false);

            final Schema avroSchema = fieldAssembler.endRecord();

            final String outputTagID = String.format("%s__%s", sanitizedDatabaseName, sanitizedCollectionName);
            final OutputTag<String> outputTag = new OutputTag<>(outputTagID) {};

            tagSchemaMap.put(sanitizedCollectionName, Tuple2.of(outputTag, avroSchema));

            LOG.debug(">>> [MONGO-STREAMER] AVRO SCHEMA INFERRED FROM 100 SAMPLES");
            LOG.debug(avroSchema.toString(true));
        }

        this.tagSchemaMap = tagSchemaMap;

        return tagSchemaMap;
    }
}
