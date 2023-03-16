package com.nju.allinplantern.flink.utils;

import com.nju.allinplantern.flink.pojo.eventbody.Sjyh;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;


public class SjyhCkUtil extends RichSinkFunction<Sjyh> {
    // ck 连接
    private ClickHouseConnection connection;

    private PreparedStatement preparedStatement;

    // 对应的 sql
    private static final String sql = "INSERT INTO dm_v_tr_sjyh_mx(uid,mch_channel,login_type,ebank_cust_no,tran_date,tran_time,tran_code,tran_sts,return_code,return_msg,sys_type,payer_acct_no,payer_acct_name,payee_acct_no,payee_acct_name,tran_amt,etl_dt) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
    public void invoke(Sjyh value, Context context) throws Exception {
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
            preparedStatement.setString(1, value.getUid());
            preparedStatement.setString(2, value.getMch_channel());
            preparedStatement.setString(3, value.getLogin_type());
            preparedStatement.setString(4, value.getEbank_cust_no());
            preparedStatement.setString(5, value.getTran_date());
            preparedStatement.setString(6, value.getTran_time());
            preparedStatement.setString(7, value.getTran_code());
            preparedStatement.setString(8, value.getTran_sts());
            preparedStatement.setString(9, value.getReturn_code());
            preparedStatement.setString(10, value.getReturn_msg());
            preparedStatement.setString(11, value.getSys_type());
            preparedStatement.setString(12, value.getPayer_acct_no());
            preparedStatement.setString(13, value.getPayer_acct_name());
            preparedStatement.setString(14, value.getPayee_acct_no());
            preparedStatement.setString(15, value.getPayee_acct_name());
            preparedStatement.setBigDecimal(16, value.getTran_amt());
            preparedStatement.setString(17, value.getEtl_dt());

            preparedStatement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
