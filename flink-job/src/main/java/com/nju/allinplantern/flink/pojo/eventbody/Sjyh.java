package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 手机银行交易
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Sjyh extends EventBody {
    /**
     * 证件号码
     */
    private String uid;

    /**
     * 模块渠道代号
     */
    private String mch_channel;

    /**
     * 登录类型
     */
    private String login_type;

    /**
     * 电子银行客户号
     */
    private String ebank_cust_no;

    /**
     * 交易日期
     */
    private String tran_date;

    /**
     * 交易时间
     */
    private String tran_time;

    /**
     * 交易代码
     */
    private String tran_code;

    /**
     * 交易状态
     */
    private String tran_sts;

    /**
     * 返回码
     */
    private String return_code;

    /**
     * 返回信息
     */
    private String return_msg;

    /**
     * 业务系统类型
     */
    private String sys_type;

    /**
     * 付款人账号
     */
    private String payer_acct_no;

    /**
     * 转出户名
     */
    private String payer_acct_name;

    /**
     * 收款人账号
     */
    private String payee_acct_no;

    /**
     * 收款人户名
     */
    private String payee_acct_name;

    /**
     * 交易金额
     */
    private BigDecimal tran_amt;

    /**
     * 数据日期
     */
    private String etl_dt;


    @Override
    public boolean isValid() {
        return false;
    }
}
