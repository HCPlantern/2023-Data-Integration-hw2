package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 合同明细
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Contract extends EventBody {
    /**
     * 证件号码
     */
    private String uid;

    /**
     * 贷款合同号
     */
    private String contract_no;

    /**
     * 相关申请流水号
     */
    private String apply_no;

    /**
     * 人工编号
     */
    private String artificial_no;

    /**
     * 发生日期
     */
    private String occur_date;

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
     * 发生类型
     */
    private String occur_type;

    /**
     * 额度是否循环
     */
    private String is_credit_cyc;

    /**
     * 币种
     */
    private String curr_type;

    /**
     * 金额
     */
    private BigDecimal buss_amt;

    /**
     * 贷款成数
     */
    private BigDecimal loan_pert;

    /**
     * 期限年
     */
    private Integer term_year;

    /**
     * 期限月
     */
    private Integer term_mth;

    /**
     * 期限日
     */
    private Integer term_day;

    /**
     * 基准利率类型
     */
    private String base_rate_type;

    /**
     * 基准利率
     */
    private BigDecimal base_rate;

    /**
     * 浮动类型
     */
    private String float_type;

    /**
     * 利率浮动
     */
    private BigDecimal rate_float;

    /**
     * 利率
     */
    private BigDecimal rate;

    /**
     * 还款期次
     */
    private Integer pay_times;

    /**
     * 还款方式
     */
    private String pay_type;

    /**
     * 投向
     */
    private String direction;

    /**
     * 用途
     */
    private String loan_use;

    /**
     * 还款来源
     */
    private String pay_source;

    /**
     * 发放日期
     */
    private String putout_date;

    /**
     * 到期日期
     */
    private String matu_date;

    /**
     * 主要担保方式
     */
    private String vouch_type;

    /**
     * 申请方式
     */
    private String apply_type;

    /**
     * 展期次数
     */
    private Integer extend_times;

    /**
     * 已实际出帐金额
     */
    private BigDecimal actu_out_amt;

    /**
     * 余额
     */
    private BigDecimal bal;

    /**
     * 正常余额
     */
    private BigDecimal norm_bal;

    /**
     * 逾期余额
     */
    private BigDecimal dlay_bal;

    /**
     * 呆滞余额
     */
    private BigDecimal dull_bal;

    /**
     * 表内欠息金额
     */
    private BigDecimal owed_int_in;

    /**
     * 表外欠息余额
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
     * 五级分类
     */
    private String five_class;

    /**
     * 最新风险分类时间
     */
    private String class_date;

    /**
     * 管户机构号
     */
    private String mge_org;

    /**
     * 管户人工号
     */
    private String mgr_no;

    /**
     * 经办机构
     */
    private String operate_org;

    /**
     * 经办人
     */
    private String operator;

    /**
     * 经办日期
     */
    private String operate_date;

    /**
     * 登记机构
     */
    private String reg_org;

    /**
     * 登记人
     */
    private String register;

    /**
     * 登记日期
     */
    private String reg_date;

    /**
     * 结息方式
     */
    private String inte_settle_type;

    /**
     * 不良记录标志
     */
    private String is_bad;

    /**
     * 冻结金额
     */
    private BigDecimal frz_amt;

    /**
     * 合同控制方式
     */
    private String con_crl_type;

    /**
     * 移交类型
     */
    private String shift_type;

    /**
     * 欠息天数
     */
    private Integer due_intr_days;

    /**
     * 原因类型
     */
    private String reson_type;

    /**
     * 移交余额
     */
    private BigDecimal shift_bal;

    /**
     * 是否担保公司担保
     */
    private String is_vc_vouch;

    /**
     * 贷款用途补充
     */
    private String loan_use_add;

    /**
     * 终结类型
     */
    private String finsh_type;

    /**
     * 终结日期
     */
    private String finsh_date;

    /**
     * 转建行标志
     */
    private String sts_flag;

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
