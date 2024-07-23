package org.example.utils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public abstract class AvroUtils {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    public static FieldAssembler<Schema> createFieldAssemblerWithFieldTypes(Map<String, Class<?>> fieldTypes) {
        FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record("Record").fields();

        for (Map.Entry<String, Class<?>> entry : fieldTypes.entrySet()) {
            String fieldName = entry.getKey();
            String sanitizedFieldName = Sanitizer.sanitize(fieldName);
            Class<?> fieldType = entry.getValue();

            addFieldToFieldAssembler(fieldAssembler, sanitizedFieldName, fieldType, true);
        }

        return fieldAssembler;
    }

    public static Schema getAvroSchemaFrom(Class<?> javaClass) {
        switch (javaClass.getSimpleName()) {
            case "ObjectId":
            case "String":
            case "Document":
                return Schema.create(Schema.Type.STRING);
            case "Integer":
                return Schema.create(Schema.Type.INT);
            case "Long":
                return Schema.create(Schema.Type.LONG);
            case "Double":
                return Schema.create(Schema.Type.DOUBLE);
            case "Boolean":
                return Schema.create(Schema.Type.BOOLEAN);
            default:
                LOG.warn(
                    ">>> [AVRO-SCHEMA-CONVERTER] UNKNOWN DATA TYPE DEFAULTED TO STRING: {}",
                    javaClass.getSimpleName()
                );
                return Schema.create(Schema.Type.STRING); // Default to STRING for unrecognized types
        }
    }

    public static void addFieldToFieldAssembler(
        FieldAssembler<Schema> fieldAssembler,
        String fieldName, Class<?> fieldType, boolean isNullable
    ) {
        LOG.debug(">>> [AVRO-SCHEMA-CONVERTER] ADDING FIELD: {} ({})", fieldName, fieldType.getSimpleName());
        FieldBuilder<Schema> fieldBuilder = fieldAssembler.name(fieldName);

        if (isNullable) {
            fieldBuilder.type().unionOf().nullType().and().type(getAvroSchemaFrom(fieldType)).endUnion().nullDefault();
        } else {
            fieldBuilder.type(getAvroSchemaFrom(fieldType)).noDefault();
        }
    }
}
