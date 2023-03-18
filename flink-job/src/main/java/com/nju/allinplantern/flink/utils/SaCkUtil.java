package com.nju.allinplantern.flink.utils;

import com.nju.allinplantern.flink.pojo.eventbody.Sa;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;


public class SaCkUtil extends RichSinkFunction<Sa> {
    private static final String url = "jdbc:clickhouse://clickhouse:8123/dm";

    // ck 连接
    private ClickHouseConnection connection;

    private PreparedStatement preparedStatement;

    // 对应的 sql
    private static final String sql = "INSERT INTO dm_v_tr_sa_mx(uid,card_no,cust_name,acct_no,det_n,curr_type,tran_teller_no,cr_amt,bal,tran_amt,tran_card_no,tran_type,tran_log_no,dr_amt,open_org,dscrp_code,remark,tran_time,tran_date,sys_date,tran_code,remark_1,oppo_cust_name,agt_cert_type,agt_cert_no,agt_cust_name,channel_flag,oppo_acct_no,oppo_bank_no,src_dt,etl_dt) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
    public void invoke(Sa value, Context context) throws Exception {
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
            preparedStatement.setString(1, value.getUid());
            preparedStatement.setString(2, value.getCard_no());
            preparedStatement.setString(3, value.getCust_name());
            preparedStatement.setString(4, value.getAcct_no());
            preparedStatement.setInt(5, value.getDet_n());
            preparedStatement.setString(6, value.getCurr_type());
            preparedStatement.setString(7, value.getTran_teller_no());
            preparedStatement.setBigDecimal(8, value.getCr_amt());
            preparedStatement.setBigDecimal(9, value.getBal());
            preparedStatement.setBigDecimal(10, value.getTran_amt());
            preparedStatement.setString(11, value.getTran_card_no());
            preparedStatement.setString(12, value.getTran_type());
            preparedStatement.setString(13, value.getTran_log_no());
            preparedStatement.setBigDecimal(14, value.getDr_amt());
            preparedStatement.setString(15, value.getOpen_org());
            preparedStatement.setString(16, value.getDscrp_code());
            preparedStatement.setString(17, value.getRemark());
            preparedStatement.setString(18, value.getTran_time());
            preparedStatement.setString(19, value.getTran_date());
            preparedStatement.setString(20, value.getSys_date());
            preparedStatement.setString(21, value.getTran_code());
            preparedStatement.setString(22, value.getRemark_1());
            preparedStatement.setString(23, value.getOppo_cust_name());
            preparedStatement.setString(24, value.getAgt_cert_type());
            preparedStatement.setString(25, value.getAgt_cert_no());
            preparedStatement.setString(26, value.getAgt_cust_name());
            preparedStatement.setString(27, value.getChannel_flag());
            preparedStatement.setString(28, value.getOppo_acct_no());
            preparedStatement.setString(29, value.getOppo_bank_no());
            preparedStatement.setString(30, value.getSrc_dt());
            preparedStatement.setString(31, value.getEtl_dt());

            preparedStatement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
