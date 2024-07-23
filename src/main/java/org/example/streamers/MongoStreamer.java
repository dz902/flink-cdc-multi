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
import org.example.deserializers.MongoAvroDebeziumDeserializer;
import org.example.utils.AvroUtils;
import org.example.utils.NoOverwriteHashMap;
import org.example.utils.Thrower;
import org.example.utils.Validator;

import java.util.HashMap;
import java.util.Map;

public class MongoStreamer implements Streamer {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private final String hosts;
    private final String databaseName;
    private final String collectionName;
    private final String username;
    private final String password;
    private final String offsetValue;

    public MongoStreamer(JSONObject configJSON) {
        // TODO: SUPPORT DB LEVEL CDC FOR MONGODB v4+
        LOG.warn(">>> [MONGO-STREAMER] CURRENTLY ONLY SINGLE DB AND COLLECTION IS SUPPORTED");
        LOG.warn(">>> [MONGO-STREAMER] BECAUSE THIS TOOL IS DEVELOPED AGAINST MONGO V3.6");

        this.hosts = Validator.ensureNotEmpty("source.hosts", configJSON.getString("source.hosts"));
        this.databaseName = Validator.ensureNotEmpty("source.database.name", configJSON.getString("source.database.name"));
        this.collectionName = Validator.ensureNotEmpty("source.collection.name", configJSON.getString("source.collection.name"));
        this.username = configJSON.getString("source.username");
        this.password = configJSON.getString("source.password");
        this.offsetValue = configJSON.getString("offset.value");

        if (this.databaseName.matches("(?i)^(?:admin|config|local)$")) {
            Thrower.errAndThrow(
                "MONGO-STREAMER",
                String.format("FLINK CDC CANNOT STREAM FROM SYSTEM DB: %s", this.databaseName)
            );
        }

        if (!this.collectionName.contains(".")) {
            Thrower.errAndThrow(
                "MONGO-STREAMER",
                String.format("COLLECTION NAME MUST BE PREFIXED WITH DB (DB.COLLECTION): %s", this.collectionName)
            );
        }

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
            .collectionList(collectionName)
            .deserializer(new MongoAvroDebeziumDeserializer())
            .startupOptions(startupOptions)
            .build();
    }

    public Map<String, Tuple2<OutputTag<String>, Schema>> getAvroSchemaMap() {
        // DB.TABLE -> (OUTPUT-TAG, SCHEMA)
        Map<String, Tuple2<OutputTag<String>, Schema>> tableTagSchemaMap = new HashMap<>();

        // Connect to MongoDB
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017/my-cdc")) {
            MongoDatabase database = mongoClient.getDatabase("my-cdc");
            MongoCollection<Document> collection = database.getCollection("my_cdc");

            // Sample size
            int sampleSize = 100;

            // Set to store field names

            Map<String, Class<?>> fieldTypes = new NoOverwriteHashMap<>();
            try (MongoCursor<Document> cursor = collection.find().limit(sampleSize).iterator()) {
                while (cursor.hasNext()) {
                    Document doc = cursor.next();
                    for (Map.Entry<String, Object> entry : doc.entrySet()) {
                        String fieldName = entry.getKey();
                        Object value = entry.getValue();
                        if (!fieldTypes.containsKey(fieldName)) {
                            fieldTypes.put(fieldName, value != null ? value.getClass() : Object.class);
                        }
                    }
                }
            }

            FieldAssembler<Schema> fieldAssembler = AvroUtils.createFieldAssemblerWithFieldTypes(fieldTypes);

            AvroUtils.addFieldToFieldAssembler(fieldAssembler, "_op", String.class, false);
            AvroUtils.addFieldToFieldAssembler(fieldAssembler, "_db", String.class, false);
            AvroUtils.addFieldToFieldAssembler(fieldAssembler, "_coll", String.class, false);
            AvroUtils.addFieldToFieldAssembler(fieldAssembler, "_ts", Long.class, false);

            Schema avroSchema = fieldAssembler.endRecord();

            // Print Avro schema
            System.out.println(avroSchema.toString(true));
        }

        return tableTagSchemaMap;
    }
}
