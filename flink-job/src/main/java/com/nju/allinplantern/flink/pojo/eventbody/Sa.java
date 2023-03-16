package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 活期交易
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Sa extends EventBody {
    /**
     * 证件号码
     */
    private String uid;

    /**
     * 卡号
     */
    private String card_no;

    /**
     * 客户名称
     */
    private String cust_name;

    /**
     * 账号
     */
    private String acct_no;

    /**
     * 活存帐户明细号
     */
    private Integer det_n;

    /**
     * 币种
     */
    private String curr_type;

    /**
     * 操作柜员号
     */
    private String tran_teller_no;

    /**
     * 贷方发生额
     */
    private BigDecimal cr_amt;

    /**
     * 余额
     */
    private BigDecimal bal;

    /**
     * 交易金额
     */
    private BigDecimal tran_amt;

    /**
     * 交易卡号
     */
    private String tran_card_no;

    /**
     * 交易类型
     */
    private String tran_type;

    /**
     * 交易流水号
     */
    private String tran_log_no;

    /**
     * 借方发生额
     */
    private BigDecimal dr_amt;

    /**
     * 开户机构号
     */
    private String open_org;

    /**
     * 摘要
     */
    private String dscrp_code;

    /**
     * 备注
     */
    private String remark;

    /**
     * 交易时间
     */
    private String tran_time;

    /**
     * 交易日期
     */
    private String tran_date;

    /**
     * 系统日期
     */
    private String sys_date;

    /**
     * 交易代码
     */
    private String tran_code;

    /**
     * 备注_1
     */
    private String remark_1;

    /**
     * 对方户名
     */
    private String oppo_cust_name;

    /**
     * 代理人证件种类
     */
    private String agt_cert_type;

    /**
     * 代理人证件号
     */
    private String agt_cert_no;

    /**
     * 代理人名称
     */
    private String agt_cust_name;

    /**
     * 渠道标志
     */
    private String channel_flag;

    /**
     * 对方账号
     */
    private String oppo_acct_no;

    /**
     * 对方行号
     */
    private String oppo_bank_no;

    /**
     * 源系统日期
     */
    private String src_dt;

    /**
     * 数据日期
     */
    private String etl_dt;


    @Override
    public boolean isValid() {
        return false;
    }
}
