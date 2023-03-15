package com.nju.allinplantern.flink.pojo.eventbody;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * 抽象类
 * 表示交易内容
 */
public abstract class EventBody {
    // TODO: 判断是否合法
    public abstract boolean isValid();
}
