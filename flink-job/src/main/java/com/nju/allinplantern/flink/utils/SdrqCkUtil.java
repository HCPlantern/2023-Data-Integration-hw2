package com.nju.allinplantern.flink.utils;

import com.nju.allinplantern.flink.pojo.eventbody.Sdrq;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;


public class SdrqCkUtil extends RichSinkFunction<Sdrq> {
    private static final String url = "jdbc:clickhouse://clickhouse:8123/dm";

    // ck 连接
    private ClickHouseConnection connection;

    private PreparedStatement preparedStatement;

    // 对应的 sql
    private static final String sql = "INSERT INTO dm_v_tr_sdrq_mx(hosehld_no,acct_no,cust_name,tran_type,tran_date,tran_amt_fen,channel_flg,tran_org,tran_teller_no,tran_log_no,batch_no,tran_sts,return_msg,etl_dt,uid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
    public void invoke(Sdrq value, Context context) throws Exception {
        // 具体的sink处理
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
            preparedStatement.setString(1, value.getHosehld_no());
            preparedStatement.setString(2, value.getAcct_no());
            preparedStatement.setString(3, value.getCust_name());
            preparedStatement.setString(4, value.getTran_type());
            preparedStatement.setString(5, value.getTran_date());
            preparedStatement.setBigDecimal(6, value.getTran_amt_fen());
            preparedStatement.setString(7, value.getChannel_flg());
            preparedStatement.setString(8, value.getTran_org());
            preparedStatement.setString(9, value.getTran_teller_no());
            preparedStatement.setString(10, value.getTran_log_no());
            preparedStatement.setString(11, value.getBatch_no());
            preparedStatement.setString(12, value.getTran_sts());
            preparedStatement.setString(13, value.getReturn_msg());
            preparedStatement.setString(14, value.getEtl_dt());
            preparedStatement.setString(15, value.getUid());

            preparedStatement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
