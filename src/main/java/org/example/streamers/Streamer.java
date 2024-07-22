package org.example.streamers;

import com.ververica.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.api.connector.source.Source;

public interface Streamer {
    Source<String, SourceSplitBase, PendingSplitsState> getSource();
}
