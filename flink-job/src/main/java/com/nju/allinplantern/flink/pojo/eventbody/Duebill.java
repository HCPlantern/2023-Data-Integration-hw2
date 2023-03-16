package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 借据明细
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Duebill extends EventBody {
    /**
     * 证件号码
     */
    private String uid;

    /**
     * 账号
     */
    private String acct_no;

    /**
     * 借据流水号
     */
    private String receipt_no;

    /**
     * 贷款合同号
     */
    private String contract_no;

    /**
     * 科目号
     */
    private String subject_no;

    /**
     * 核心客户号
     */
    private String cust_no;

    /**
     * 信贷客户号
     */
    private String loan_cust_no;

    /**
     * 客户名称
     */
    private String cust_name;

    /**
     * 业务品种
     */
    private String buss_type;

    /**
     * 币种
     */
    private String curr_type;

    /**
     * 金额
     */
    private BigDecimal buss_amt;

    /**
     * 发放日期
     */
    private String putout_date;

    /**
     * 约定到期日
     */
    private String matu_date;

    /**
     * 执行到期日
     */
    private String actu_matu_date;

    /**
     * 利率
     */
    private BigDecimal buss_rate;

    /**
     * 执行利率
     */
    private BigDecimal actu_buss_rate;

    /**
     * 计息方式
     */
    private String intr_type;

    /**
     * 计息周期
     */
    private String intr_cyc;

    /**
     * 还款期次
     */
    private Integer pay_times;

    /**
     * 还款周期
     */
    private String pay_cyc;

    /**
     * 展期次数
     */
    private Integer extend_times;

    /**
     * 余额
     */
    private BigDecimal bal;

    /**
     * 正常余额
     */
    private BigDecimal norm_bal;

    /**
     * 逾期金额
     */
    private BigDecimal dlay_amt;

    /**
     * 呆滞金额
     */
    private BigDecimal dull_amt;

    /**
     * 呆帐金额
     */
    private BigDecimal bad_debt_amt;

    /**
     * 表内欠息金额
     */
    private BigDecimal owed_int_in;

    /**
     * 表外欠息金额
     */
    private BigDecimal owed_int_out;

    /**
     * 本金罚息
     */
    private BigDecimal fine_pr_int;

    /**
     * 利息罚息
     */
    private BigDecimal fine_intr_int;

    /**
     * 逾期天数
     */
    private Integer dlay_days;

    /**
     * 存款帐号
     */
    private String pay_acct;

    /**
     * 放款账号
     */
    private String putout_acct;

    /**
     * 还款帐号
     */
    private String pay_back_acct;

    /**
     * 欠息天数
     */
    private Integer due_intr_days;

    /**
     * 经办机构
     */
    private String operate_org;

    /**
     * 经办人
     */
    private String operator;

    /**
     * 登记机构
     */
    private String reg_org;

    /**
     * 登记人
     */
    private String register;

    /**
     * 发生日期
     */
    private String occur_date;

    /**
     * 贷款用途
     */
    private String loan_use;

    /**
     * 还款方式
     */
    private String pay_type;

    /**
     * 还款频率
     */
    private String pay_freq;

    /**
     * 主要担保方式
     */
    private String vouch_type;

    /**
     * 管户人工号
     */
    private String mgr_no;

    /**
     * 管户机构号
     */
    private String mge_org;

    /**
     * 贷款渠道
     */
    private String loan_channel;

    /**
     * 新十级分类编码
     */
    private String ten_class;

    /**
     * 源系统日期
     */
    private String src_dt;

    /**
     * 平台日期
     */
    private String etl_dt;


    @Override
    public boolean isValid() {
        return false;
    }
}
