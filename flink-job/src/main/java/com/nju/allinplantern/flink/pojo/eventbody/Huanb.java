package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 贷款还本明细
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Huanb extends EventBody {
    /**
     * 还本标志
     */
    private String tran_flag;

    /**
     * 证件号码
     */
    private String uid;

    /**
     * 客户名称
     */
    private String cust_name;

    /**
     * 账号
     */
    private String acct_no;

    /**
     * 交易日期
     */
    private String tran_date;

    /**
     * 交易时间
     */
    private String tran_time;

    /**
     * 交易金额
     */
    private BigDecimal tran_amt;

    /**
     * 余额
     */
    private BigDecimal bal;

    /**
     * 交易代码
     */
    private String tran_code;

    /**
     * 借贷别
     */
    private String dr_cr_code;

    /**
     * 还款期数
     */
    private Integer pay_term;

    /**
     * 操作柜员号
     */
    private String tran_teller_no;

    /**
     * 每期还款金额
     */
    private BigDecimal pprd_rfn_amt;

    /**
     * 每期摊还额计算利息
     */
    private BigDecimal pprd_amotz_intr;

    /**
     * 交易流水号
     */
    private String tran_log_no;

    /**
     * 交易类型
     */
    private String tran_type;

    /**
     * 摘要
     */
    private String dscrp_code;

    /**
     * 备注
     */
    private String remark;

    /**
     * 数据日期
     */
    private String etl_dt;


    @Override
    public boolean isValid() {
        return false;
    }
}
