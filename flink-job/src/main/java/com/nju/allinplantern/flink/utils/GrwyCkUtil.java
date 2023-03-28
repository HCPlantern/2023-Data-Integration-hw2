package com.nju.allinplantern.flink.utils;

import com.nju.allinplantern.flink.pojo.eventbody.Grwy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;


public class GrwyCkUtil extends RichSinkFunction<Grwy> {
    // ck 连接
    private ClickHouseConnection connection;

    private PreparedStatement preparedStatement;

    // 对应的 sql
    private static final String sql = "INSERT INTO dm_v_tr_grwy_mx(uid,mch_channel,login_type,ebank_cust_no,tran_date,tran_time,tran_code,tran_sts,return_code,return_msg,sys_type,payer_acct_no,payer_acct_name,payee_acct_no,payee_acct_name,tran_amt,etl_dt) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    // 数据条目计数器
    private static int count = 0;

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
    public void invoke(Grwy value, Context context) throws Exception {
        // 具体的sink处理
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser("default");
        properties.setPassword("16d808ef");
        properties.setSessionId("default-session-id");

        ClickHouseDataSource dataSource = new ClickHouseDataSource(Constant.getInstance().url, properties);
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


            preparedStatement.addBatch();

            ++count;
            ++Constant.totalCount;
            int[] successLines;
            if (count % Constant.INSERT_BATCH_SIZE == 0) { //可能会丢最后几条(小于INSERT_BATCH_SIZE条)
                successLines = preparedStatement.executeBatch();
                //提交，批量插入数据库中
                connection.commit();
                preparedStatement.clearBatch();
                //这里统计的是该type的插入量
//              if (count % Constant.INSERT_LOG_SIZE == 0)
//                  System.out.println("dm.dm_v_tr_grwy_mx：第" + count + "条数据，" + "成功了插入了" +
//                          successLines.length + "行数据");
            }
            if (Constant.totalCount % Constant.INSERT_LOG_SIZE == 0) {
                System.out.println("共已插入 " + Constant.totalCount + " 条数据");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
