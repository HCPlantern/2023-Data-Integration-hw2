package com.nju.allinplantern.flink.utils;

import com.nju.allinplantern.flink.pojo.eventbody.Shop;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;


public class ShopCkUtil extends RichSinkFunction<Shop> {
    // ck 连接
    private ClickHouseConnection connection;

    private PreparedStatement preparedStatement;

    // 对应的 sql
    private static final String sql = "INSERT INTO dm_v_tr_shop_mx(tran_channel,order_code,shop_code,shop_name,hlw_tran_type,tran_date,tran_time,tran_amt,current_status,score_num,pay_channel,uid,legal_name,etl_dt) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(Shop value, Context context) throws Exception {
        // 具体的sink处理
        String url = "jdbc:clickhouse://172.17.188.153:8123";
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser("default");
        properties.setPassword("16d808ef");
        properties.setSessionId("default-session-id");

        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        Map<ClickHouseQueryParam, String> additionalDBParams = new HashMap<>();
        additionalDBParams.put(ClickHouseQueryParam.SESSION_ID, "new-session-id");
        try {
            if (connection == null) {
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
                preparedStatement = connection.prepareStatement(sql);
            } else {
                System.out.println("无需重新建立连接");
            }
            preparedStatement.setString(1, value.getTran_channel());
            preparedStatement.setString(2, value.getOrder_code());
            preparedStatement.setString(3, value.getShop_code());
            preparedStatement.setString(4, value.getShop_name());
            preparedStatement.setString(5, value.getHlw_tran_type());
            preparedStatement.setString(6, value.getTran_date());
            preparedStatement.setString(7, value.getTran_time());
            preparedStatement.setBigDecimal(8, value.getTran_amt());
            preparedStatement.setString(9, value.getCurrent_status());
            preparedStatement.setBigDecimal(10, value.getScore_num());
            preparedStatement.setString(11, value.getPay_channel());
            preparedStatement.setString(12, value.getUid());
            preparedStatement.setString(13, value.getLegal_name());
            preparedStatement.setString(14, value.getEtl_dt());

            preparedStatement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
