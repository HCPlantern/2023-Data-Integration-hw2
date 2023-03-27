package com.nju.allinplantern.flink.utils;

public class Constant {
    public static final String url = PropertyFactory.getProperties().getProperty("database.schema");
    public static final String topic = PropertyFactory.getProperties().getProperty("topic");
}
