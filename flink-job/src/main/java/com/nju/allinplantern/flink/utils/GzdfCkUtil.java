package com.nju.allinplantern.flink.utils;

import com.nju.allinplantern.flink.pojo.eventbody.Gzdf;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;


public class GzdfCkUtil extends RichSinkFunction<Gzdf> {
    // ck 连接
    private ClickHouseConnection connection;

    private PreparedStatement preparedStatement;

    // 对应的 sql
    private static final String sql = "INSERT INTO dm_v_tr_gzdf_mx(belong_org,ent_acct,ent_name,eng_cert_no,acct_no,cust_name,uid,tran_date,tran_amt,tran_log_no,is_secu_card,trna_channel,batch_no,etl_dt) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
    public void invoke(Gzdf value, Context context) throws Exception {
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
            preparedStatement.setString(1, value.getBelong_org());
            preparedStatement.setString(2, value.getEnt_acct());
            preparedStatement.setString(3, value.getEnt_name());
            preparedStatement.setString(4, value.getEng_cert_no());
            preparedStatement.setString(5, value.getAcct_no());
            preparedStatement.setString(6, value.getCust_name());
            preparedStatement.setString(7, value.getUid());
            preparedStatement.setString(8, value.getTran_date());
            preparedStatement.setBigDecimal(9, value.getTran_amt());
            preparedStatement.setString(10, value.getTran_log_no());
            preparedStatement.setString(11, value.getIs_secu_card());
            preparedStatement.setString(12, value.getTrna_channel());
            preparedStatement.setString(13, value.getBatch_no());
            preparedStatement.setString(14, value.getEtl_dt());

            preparedStatement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
