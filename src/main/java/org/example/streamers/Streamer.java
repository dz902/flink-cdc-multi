package org.example.streamers;

import com.ververica.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.avro.Schema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.OutputTag;

import java.util.Map;

public interface Streamer {
    Source<String, SourceSplitBase, PendingSplitsState> getSource();
    Map<String, Tuple2<OutputTag<String>, Schema>> getAvroSchemaMap();
}
