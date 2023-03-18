package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 第三方交易
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Dsf extends EventBody {
    /**
     * 交易日期
     */
    private String tran_date;

    /**
     * 交易流水号
     */
    private String tran_log_no;

    /**
     * 交易代码
     */
    private String tran_code;

    /**
     * 渠道
     */
    private String channel_flg;

    /**
     * 交易机构号
     */
    private String tran_org;

    /**
     * 操作柜员号
     */
    private String tran_teller_no;

    /**
     * 借贷方标识
     */
    private String dc_flag;

    /**
     * 交易金额
     */
    private BigDecimal tran_amt;

    /**
     * 发起行行号
     */
    private String send_bank;

    /**
     * 付款人开户行行号
     */
    private String payer_open_bank;

    /**
     * 付款人账号
     */
    private String payer_acct_no;

    /**
     * 付款人名称
     */
    private String payer_name;

    /**
     * 收款人开户行行号
     */
    private String payee_open_bank;

    /**
     * 收款人账号
     */
    private String payee_acct_no;

    /**
     * 收款人名称
     */
    private String payee_name;

    /**
     * 交易状态
     */
    private String tran_sts;

    /**
     * 业务类型
     */
    private String busi_type;

    /**
     * 业务种类
     */
    private String busi_sub_type;

    /**
     * 数据日期
     */
    private String etl_dt;

    /**
     * 证件号码
     */
    private String uid;


    @Override
    public boolean isValid() {
        return false;
    }
}
