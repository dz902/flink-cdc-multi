package org.example.utils;

import org.apache.flink.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Validator {
    private static final Logger LOG = LogManager.getLogger("flink-cdc-multi");

    public static String ensureNotEmpty(String what, String str) {
        if (StringUtils.isNullOrWhitespaceOnly(str)) {
            Thrower.errAndThrow("VALIDATOR", String.format("CANNOT BE EMPTY: %s", what));
        }

        return str;
    }

    public static <T> T withDefault(T thing, T defaultValue) {
        return thing == null ? defaultValue : thing;
    }
}
