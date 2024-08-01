package org.example.richmapfunctions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JSONToGenericRecordMapFunction extends RichMapFunction<String, GenericRecord> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    private final String schemaString;
    private transient Schema avroSchema;

    public JSONToGenericRecordMapFunction(String schemaString) {
        this.schemaString = schemaString;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.avroSchema = new Schema.Parser().parse(schemaString);
    }

    @Override
    public GenericRecord map(String value) throws Exception {
        LOG.debug(">>> [JSON-TO-AVRO] CONVERT JSON STRING TO AVRO GENERIC RECORD");
        LOG.trace(value);
        LOG.trace(avroSchema);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
        Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema, value);
        return reader.read(null, decoder);
    }
}
