package com.nju.allinplantern.flink.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

public class PropertyFactory {
    private static Properties properties;


    public static Properties getProperties() {
        if (Objects.isNull(properties)) {
            properties = new Properties();
            try {
                InputStream in = ClassLoader.getSystemResourceAsStream("flink-kafka-consumer.properties");
                properties.load(in);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return properties;
    }
}
