package org.example.streamers;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Map;

// FOR SOME REASON KEYED FUNC WILL TRY TO SERIALIZE ITS PARENT WHICH IS THIS CLASS
// SO WE NEED SERIALIZABLE
public interface Streamer<T> extends Serializable {
    Source<T, ?, ?> getSource();
    Map<String, Tuple2<OutputTag<String>, String>> createTagSchemaMap();
    SingleOutputStreamOperator<String> createMainDataStream(DataStream<String> sourceStream);
}
