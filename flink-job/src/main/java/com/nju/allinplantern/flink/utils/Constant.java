package com.nju.allinplantern.flink.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class Constant {

    private static Constant instance = new Constant();

    public String url = "jdbc:clickhouse://clickhouse:8123/stream_test";

    public String topic = "test";

    public Properties properties;

    /**
     * 批量插入数
     */
    public static final int INSERT_BATCH_SIZE = 500;

    /**
     * 日志输出步长
     */
    public static final int INSERT_LOG_SIZE = 10000;

    /**
     * 总数据条目已插入数
     */
    public static int totalCount = 0;

    private Constant() {
    }

    public static Constant getInstance() {
        return instance;
    }

    public void initialize(String propertyPath) {
        properties = new Properties();
        try {
            properties.load(Files.newBufferedReader(Paths.get(propertyPath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
