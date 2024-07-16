package org.example;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.util.Preconditions;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DatabaseTableDateBucketAssigner implements BucketAssigner<GenericRecord, String> {

    //private static final long serialVersionUID = 1L;

    private static final String DATE_FORMAT = "yyyy-MM-dd";

    private final ZoneId zoneId;

    private transient DateTimeFormatter dateFormatter;

    public DatabaseTableDateBucketAssigner() {
        this(ZoneId.systemDefault());
    }

    public DatabaseTableDateBucketAssigner(ZoneId zoneId) {
        this.zoneId = Preconditions.checkNotNull(zoneId);
    }

    @Override
    public String getBucketId(GenericRecord record, Context context) {
        if (dateFormatter == null) {
            dateFormatter = DateTimeFormatter.ofPattern(DATE_FORMAT).withZone(zoneId);
        }

        long ts = (long) record.get("_ts");
        String date = dateFormatter.format(Instant.ofEpochMilli(ts));

        return String.format("/dt=%s", date);
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
