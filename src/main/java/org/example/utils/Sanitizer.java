package org.example.utils;

public abstract class Sanitizer {
    public static String sanitize(String name) {
        return name.replace('-', '_');
    }
}
