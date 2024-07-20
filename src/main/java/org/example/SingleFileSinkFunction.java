package org.example;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

@PublicEvolving
public class SingleFileSinkFunction extends RichSinkFunction<String> {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    private transient BufferedWriter writer;
    private final Path outputPath;

    public SingleFileSinkFunction(Path outputPath) {
        this.outputPath = outputPath;
    }

//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//
//    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        LOG.debug(">>> [SINGLE-FILE-SINK] WRITING FILE TO: {}", outputPath);
        LOG.trace(value);

        FileSystem fs = outputPath.getFileSystem();
        writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath, FileSystem.WriteMode.OVERWRITE), StandardCharsets.UTF_8));
        writer.write(value);
        writer.close();
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.close();
        }
    }
}
