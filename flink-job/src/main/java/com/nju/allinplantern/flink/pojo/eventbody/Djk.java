package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 贷记卡交易
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Djk extends EventBody {
    /**
     * 证件号码
     */
    private String uid;

    /**
     * 卡号
     */
    private String card_no;

    /**
     * 交易类型
     */
    private String tran_type;

    /**
     * 交易类型描述
     */
    private String tran_type_desc;

    /**
     * 交易金额
     */
    private BigDecimal tran_amt;

    /**
     * 交易金额符号
     */
    private String tran_amt_sign;

    /**
     * 商户类型
     */
    private String mer_type;

    /**
     * 商户代码
     */
    private String mer_code;

    /**
     * 撤销、冲正标志
     */
    private String rev_ind;

    /**
     * 交易描述
     */
    private String tran_desc;

    /**
     * 交易日期
     */
    private String tran_date;

    /**
     * 入账日期
     */
    private String val_date;

    /**
     * 交易发生日期
     */
    private String pur_date;

    /**
     * 交易时间
     */
    private String tran_time;

    /**
     * 账号
     */
    private String acct_no;

    /**
     * 数据日期
     */
    private String etl_dt;


    @Override
    public boolean isValid() {
        return false;
    }
}
