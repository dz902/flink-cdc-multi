package org.example.utils;

import java.util.HashMap;

public class NoOverwriteHashMap<K, V> extends HashMap<K, V> {

    @Override
    public V put(K key, V value) {
        if (containsKey(key)) {
            Thrower.errAndThrow("[NO-OVERWRITE-MAP]", String.format("CANNOT OVERWRITE FIELD: %s", key));
        }

        return super.put(key, value);
    }
}