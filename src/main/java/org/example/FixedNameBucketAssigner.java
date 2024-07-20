package org.example;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

public class FixedNameBucketAssigner implements BucketAssigner<String, String> {
    String fixedName;
    public FixedNameBucketAssigner(String fixedName) {
        this.fixedName = fixedName;
    }

    @Override
    public String getBucketId(String record, Context context) {
        return "/" + this.fixedName;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
