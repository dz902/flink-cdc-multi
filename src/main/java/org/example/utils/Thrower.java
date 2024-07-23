package org.example.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Thrower {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");
    public static void errAndThrow(String module, String msg) throws RuntimeException {
        LOG.error(">>> [{}] {}", module, msg);
        throw new RuntimeException(msg);
    }
}
