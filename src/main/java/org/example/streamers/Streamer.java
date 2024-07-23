package org.example.streamers;

import com.ververica.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.avro.Schema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Map;

// FOR SOME REASON KEYED FUNC WILL TRY TO SERIALIZE ITS PARENT WHICH IS THIS CLASS
// SO WE NEED SERIALIZABLE
public interface Streamer extends Serializable {
    Source<String, SourceSplitBase, PendingSplitsState> getSource();
    Map<String, Tuple2<OutputTag<String>, Schema>> createTagSchemaMap();
}
