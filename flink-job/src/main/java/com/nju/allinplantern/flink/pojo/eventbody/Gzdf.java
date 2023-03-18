package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 工资代发明细
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Gzdf extends EventBody {
    /**
     * 归属机构号
     */
    private String belong_org;

    /**
     * 企业账号
     */
    private String ent_acct;

    /**
     * 企业名称
     */
    private String ent_name;

    /**
     * 企业证件号码
     */
    private String eng_cert_no;

    /**
     * 账号
     */
    private String acct_no;

    /**
     * 客户名称
     */
    private String cust_name;

    /**
     * 证件号码
     */
    private String uid;

    /**
     * 交易日期
     */
    private String tran_date;

    /**
     * 交易金额
     */
    private BigDecimal tran_amt;

    /**
     * 交易流水号
     */
    private String tran_log_no;

    /**
     * 是否社保卡
     */
    private String is_secu_card;

    /**
     * 代发渠道
     */
    private String trna_channel;

    /**
     * 批次号
     */
    private String batch_no;

    /**
     * 数据日期
     */
    private String etl_dt;


    @Override
    public boolean isValid() {
        return false;
    }
}
