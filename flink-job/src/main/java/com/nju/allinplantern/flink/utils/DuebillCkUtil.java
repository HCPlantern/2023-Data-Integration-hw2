package com.nju.allinplantern.flink.utils;

import com.nju.allinplantern.flink.pojo.eventbody.Duebill;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;


public class DuebillCkUtil extends RichSinkFunction<Duebill> {
    // ck 连接
    private ClickHouseConnection connection;

    private PreparedStatement preparedStatement;

    // 对应的 sql
    private static final String sql = "INSERT INTO dm_v_tr_duebill_mx(uid,acct_no,receipt_no,contract_no,subject_no,cust_no,loan_cust_no,cust_name,buss_type,curr_type,buss_amt,putout_date,matu_date,actu_matu_date,buss_rate,actu_buss_rate,intr_type,intr_cyc,pay_times,pay_cyc,extend_times,bal,norm_bal,dlay_amt,dull_amt,bad_debt_amt,owed_int_in,owed_int_out,fine_pr_int,fine_intr_int,dlay_days,pay_acct,putout_acct,pay_back_acct,due_intr_days,operate_org,operator,reg_org,register,occur_date,loan_use,pay_type,pay_freq,vouch_type,mgr_no,mge_org,loan_channel,ten_class,src_dt,etl_dt) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
    public void invoke(Duebill value, Context context) throws Exception {
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
            preparedStatement.setString(2, value.getAcct_no());
            preparedStatement.setString(3, value.getReceipt_no());
            preparedStatement.setString(4, value.getContract_no());
            preparedStatement.setString(5, value.getSubject_no());
            preparedStatement.setString(6, value.getCust_no());
            preparedStatement.setString(7, value.getLoan_cust_no());
            preparedStatement.setString(8, value.getCust_name());
            preparedStatement.setString(9, value.getBuss_type());
            preparedStatement.setString(10, value.getCurr_type());
            preparedStatement.setBigDecimal(11, value.getBuss_amt());
            preparedStatement.setString(12, value.getPutout_date());
            preparedStatement.setString(13, value.getMatu_date());
            preparedStatement.setString(14, value.getActu_matu_date());
            preparedStatement.setBigDecimal(15, value.getBuss_rate());
            preparedStatement.setBigDecimal(16, value.getActu_buss_rate());
            preparedStatement.setString(17, value.getIntr_type());
            preparedStatement.setString(18, value.getIntr_cyc());
            preparedStatement.setInt(19, value.getPay_times());
            preparedStatement.setString(20, value.getPay_cyc());
            preparedStatement.setInt(21, value.getExtend_times());
            preparedStatement.setBigDecimal(22, value.getBal());
            preparedStatement.setBigDecimal(23, value.getNorm_bal());
            preparedStatement.setBigDecimal(24, value.getDlay_amt());
            preparedStatement.setBigDecimal(25, value.getDull_amt());
            preparedStatement.setBigDecimal(26, value.getBad_debt_amt());
            preparedStatement.setBigDecimal(27, value.getOwed_int_in());
            preparedStatement.setBigDecimal(28, value.getOwed_int_out());
            preparedStatement.setBigDecimal(29, value.getFine_pr_int());
            preparedStatement.setBigDecimal(30, value.getFine_intr_int());
            preparedStatement.setInt(31, value.getDlay_days());
            preparedStatement.setString(32, value.getPay_acct());
            preparedStatement.setString(33, value.getPutout_acct());
            preparedStatement.setString(34, value.getPay_back_acct());
            preparedStatement.setInt(35, value.getDue_intr_days());
            preparedStatement.setString(36, value.getOperate_org());
            preparedStatement.setString(37, value.getOperator());
            preparedStatement.setString(38, value.getReg_org());
            preparedStatement.setString(39, value.getRegister());
            preparedStatement.setString(40, value.getOccur_date());
            preparedStatement.setString(41, value.getLoan_use());
            preparedStatement.setString(42, value.getPay_type());
            preparedStatement.setString(43, value.getPay_freq());
            preparedStatement.setString(44, value.getVouch_type());
            preparedStatement.setString(45, value.getMgr_no());
            preparedStatement.setString(46, value.getMge_org());
            preparedStatement.setString(47, value.getLoan_channel());
            preparedStatement.setString(48, value.getTen_class());
            preparedStatement.setString(49, value.getSrc_dt());
            preparedStatement.setString(50, value.getEtl_dt());

            preparedStatement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
