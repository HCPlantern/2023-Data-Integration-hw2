package com.nju.allinplantern.flink.utils;

import com.nju.allinplantern.flink.pojo.eventbody.Dsf;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;


public class DsfCkUtil extends RichSinkFunction<Dsf> {

    // ck 连接
    private ClickHouseConnection connection;

    private PreparedStatement preparedStatement;

    // 对应的 sql
    private static final String sql = "INSERT INTO dm_v_tr_dsf_mx(tran_date,tran_log_no,tran_code,channel_flg,tran_org,tran_teller_no,dc_flag,tran_amt,send_bank,payer_open_bank,payer_acct_no,payer_name,payee_open_bank,payee_acct_no,payee_name,tran_sts,busi_type,busi_sub_type,etl_dt,uid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
    public void invoke(Dsf value, Context context) throws Exception {
        // 具体的sink处理
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser("default");
        properties.setPassword("16d808ef");
        properties.setSessionId("default-session-id");

        ClickHouseDataSource dataSource = new ClickHouseDataSource(Constant.url, properties);
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
            preparedStatement.setString(1, value.getTran_date());
            preparedStatement.setString(2, value.getTran_log_no());
            preparedStatement.setString(3, value.getTran_code());
            preparedStatement.setString(4, value.getChannel_flg());
            preparedStatement.setString(5, value.getTran_org());
            preparedStatement.setString(6, value.getTran_teller_no());
            preparedStatement.setString(7, value.getDc_flag());
            preparedStatement.setBigDecimal(8, value.getTran_amt());
            preparedStatement.setString(9, value.getSend_bank());
            preparedStatement.setString(10, value.getPayer_open_bank());
            preparedStatement.setString(11, value.getPayer_acct_no());
            preparedStatement.setString(12, value.getPayer_name());
            preparedStatement.setString(13, value.getPayee_open_bank());
            preparedStatement.setString(14, value.getPayee_acct_no());
            preparedStatement.setString(15, value.getPayee_name());
            preparedStatement.setString(16, value.getTran_sts());
            preparedStatement.setString(17, value.getBusi_type());
            preparedStatement.setString(18, value.getBusi_sub_type());
            preparedStatement.setString(19, value.getEtl_dt());
            preparedStatement.setString(20, value.getUid());

            preparedStatement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
