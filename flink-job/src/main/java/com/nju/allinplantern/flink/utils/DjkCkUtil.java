package com.nju.allinplantern.flink.utils;

import com.nju.allinplantern.flink.pojo.eventbody.Djk;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;


public class DjkCkUtil extends RichSinkFunction<Djk> {
    // ck 连接
    private ClickHouseConnection connection;

    private PreparedStatement preparedStatement;

    // 对应的 sql
    private static final String sql = "INSERT INTO dm_v_tr_djk_mx(uid,card_no,tran_type,tran_type_desc,tran_amt,tran_amt_sign,mer_type,mer_code,rev_ind,tran_desc,tran_date,val_date,pur_date,tran_time,acct_no,etl_dt) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
    public void invoke(Djk value, Context context) throws Exception {
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
            }
            preparedStatement.setString(1, value.getUid());
            preparedStatement.setString(2, value.getCard_no());
            preparedStatement.setString(3, value.getTran_type());
            preparedStatement.setString(4, value.getTran_type_desc());
            preparedStatement.setBigDecimal(5, value.getTran_amt());
            preparedStatement.setString(6, value.getTran_amt_sign());
            preparedStatement.setString(7, value.getMer_type());
            preparedStatement.setString(8, value.getMer_code());
            preparedStatement.setString(9, value.getRev_ind());
            preparedStatement.setString(10, value.getTran_desc());
            preparedStatement.setString(11, value.getTran_date());
            preparedStatement.setString(12, value.getVal_date());
            preparedStatement.setString(13, value.getPur_date());
            preparedStatement.setString(14, value.getTran_time());
            preparedStatement.setString(15, value.getAcct_no());
            preparedStatement.setString(16, value.getEtl_dt());

            preparedStatement.addBatch();

            ++count;
            ++Constant.totalCount;
            if (count % Constant.INSERT_BATCH_SIZE == 0) { //可能会丢最后几条(小于INSERT_BATCH_SIZE条)
                preparedStatement.executeBatch();
                //提交，批量插入数据库中
                connection.commit();
                preparedStatement.clearBatch();
            }
            if (Constant.totalCount % Constant.INSERT_LOG_SIZE == 0) {
                System.out.println(System.currentTimeMillis() + ": " + "共已插入 " + Constant.totalCount + " 条数据");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
