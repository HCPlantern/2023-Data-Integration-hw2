package com.nju.allinplantern.flink.utils;

import com.nju.allinplantern.flink.pojo.eventbody.Contract;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;


public class ContractCkUtil extends RichSinkFunction<Contract> {
    private static final String url = "jdbc:clickhouse://clickhouse:8123/dm";

    // ck 连接
    private ClickHouseConnection connection;

    private PreparedStatement preparedStatement;

    // 对应的 sql
    private static final String sql = "INSERT INTO dm_v_tr_contract_mx(uid,contract_no,apply_no,artificial_no,occur_date,loan_cust_no,cust_name,buss_type,occur_type,is_credit_cyc,curr_type,buss_amt,loan_pert,term_year,term_mth,term_day,base_rate_type,base_rate,float_type,rate_float,rate,pay_times,pay_type,direction,loan_use,pay_source,putout_date,matu_date,vouch_type,apply_type,extend_times,actu_out_amt,bal,norm_bal,dlay_bal,dull_bal,owed_int_in,owed_int_out,fine_pr_int,fine_intr_int,dlay_days,five_class,class_date,mge_org,mgr_no,operate_org,operator,operate_date,reg_org,register,reg_date,inte_settle_type,is_bad,frz_amt,con_crl_type,shift_type,due_intr_days,reson_type,shift_bal,is_vc_vouch,loan_use_add,finsh_type,finsh_date,sts_flag,src_dt,etl_dt) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
    public void invoke(Contract value, Context context) throws Exception {
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
            preparedStatement.setString(2, value.getContract_no());
            preparedStatement.setString(3, value.getApply_no());
            preparedStatement.setString(4, value.getArtificial_no());
            preparedStatement.setString(5, value.getOccur_date());
            preparedStatement.setString(6, value.getLoan_cust_no());
            preparedStatement.setString(7, value.getCust_name());
            preparedStatement.setString(8, value.getBuss_type());
            preparedStatement.setString(9, value.getOccur_type());
            preparedStatement.setString(10, value.getIs_credit_cyc());
            preparedStatement.setString(11, value.getCurr_type());
            preparedStatement.setBigDecimal(12, value.getBuss_amt());
            preparedStatement.setBigDecimal(13, value.getLoan_pert());
            preparedStatement.setInt(14, value.getTerm_year());
            preparedStatement.setInt(15, value.getTerm_mth());
            preparedStatement.setInt(16, value.getTerm_day());
            preparedStatement.setString(17, value.getBase_rate_type());
            preparedStatement.setBigDecimal(18, value.getBase_rate());
            preparedStatement.setString(19, value.getFloat_type());
            preparedStatement.setBigDecimal(20, value.getRate_float());
            preparedStatement.setBigDecimal(21, value.getRate());
            preparedStatement.setInt(22, value.getPay_times());
            preparedStatement.setString(23, value.getPay_type());
            preparedStatement.setString(24, value.getDirection());
            preparedStatement.setString(25, value.getLoan_use());
            preparedStatement.setString(26, value.getPay_source());
            preparedStatement.setString(27, value.getPutout_date());
            preparedStatement.setString(28, value.getMatu_date());
            preparedStatement.setString(29, value.getVouch_type());
            preparedStatement.setString(30, value.getApply_type());
            preparedStatement.setInt(31, value.getExtend_times());
            preparedStatement.setBigDecimal(32, value.getActu_out_amt());
            preparedStatement.setBigDecimal(33, value.getBal());
            preparedStatement.setBigDecimal(34, value.getNorm_bal());
            preparedStatement.setBigDecimal(35, value.getDlay_bal());
            preparedStatement.setBigDecimal(36, value.getDull_bal());
            preparedStatement.setBigDecimal(37, value.getOwed_int_in());
            preparedStatement.setBigDecimal(38, value.getOwed_int_out());
            preparedStatement.setBigDecimal(39, value.getFine_pr_int());
            preparedStatement.setBigDecimal(40, value.getFine_intr_int());
            preparedStatement.setInt(41, value.getDlay_days());
            preparedStatement.setString(42, value.getFive_class());
            preparedStatement.setString(43, value.getClass_date());
            preparedStatement.setString(44, value.getMge_org());
            preparedStatement.setString(45, value.getMgr_no());
            preparedStatement.setString(46, value.getOperate_org());
            preparedStatement.setString(47, value.getOperator());
            preparedStatement.setString(48, value.getOperate_date());
            preparedStatement.setString(49, value.getReg_org());
            preparedStatement.setString(50, value.getRegister());
            preparedStatement.setString(51, value.getReg_date());
            preparedStatement.setString(52, value.getInte_settle_type());
            preparedStatement.setString(53, value.getIs_bad());
            preparedStatement.setBigDecimal(54, value.getFrz_amt());
            preparedStatement.setString(55, value.getCon_crl_type());
            preparedStatement.setString(56, value.getShift_type());
            preparedStatement.setInt(57, value.getDue_intr_days());
            preparedStatement.setString(58, value.getReson_type());
            preparedStatement.setBigDecimal(59, value.getShift_bal());
            preparedStatement.setString(60, value.getIs_vc_vouch());
            preparedStatement.setString(61, value.getLoan_use_add());
            preparedStatement.setString(62, value.getFinsh_type());
            preparedStatement.setString(63, value.getFinsh_date());
            preparedStatement.setString(64, value.getSts_flag());
            preparedStatement.setString(65, value.getSrc_dt());
            preparedStatement.setString(66, value.getEtl_dt());

            preparedStatement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
