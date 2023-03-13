package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * 商户交易明细
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Shop extends EventBody {
    // 是否都用 String ?
    private String tranChanel;

    private String orderCode;

    private String shopCode;

    private String shopName;

    private String hlwTranType;

    private String tranDate;

    private String tranTime;

    private String tranAmount;

    private String currentStatus;

    private String scoreNum;

    private String payChannel;

    private String uid;

    private String legalName;

    private String etlDt;

    @Override
    public boolean isValid() {
        return false;
    }

    // TODO: sink函数(优化实现 )
    public static SinkFunction<Shop> SinkCreator(String tableName, int batchSize, JdbcConnectionOptions options) {
        return null;
    }
}
