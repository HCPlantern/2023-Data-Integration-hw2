package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 水电燃气交易
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Sdrq extends EventBody {
    /**
     * 户号
     */
    private String hosehld_no;

    /**
     * 账号
     */
    private String acct_no;

    /**
     * 客户名称
     */
    private String cust_name;

    /**
     * 交易类型
     */
    private String tran_type;

    /**
     * 交易日期
     */
    private String tran_date;

    /**
     * 交易金额
     */
    private BigDecimal tran_amt_fen;

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
     * 交易流水号
     */
    private String tran_log_no;

    /**
     * 批次号
     */
    private String batch_no;

    /**
     * 交易状态
     */
    private String tran_sts;

    /**
     * 返回信息
     */
    private String return_msg;

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
