package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 社保医保交易
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Sbyb extends EventBody {
    /**
     * 证件号码
     */
    private String uid;

    /**
     * 客户名称
     */
    private String cust_name;

    /**
     * 交易日期
     */
    private String tran_date;

    /**
     * 交易状态
     */
    private String tran_sts;

    /**
     * 交易机构号
     */
    private String tran_org;

    /**
     * 操作柜员号
     */
    private String tran_teller_no;

    /**
     * 交易金额
     */
    private BigDecimal tran_amt_fen;

    /**
     * 交易类型
     */
    private String tran_type;

    /**
     * 返回信息
     */
    private String return_msg;

    /**
     * 数据日期
     */
    private String etl_dt;


    @Override
    public boolean isValid() {
        return false;
    }
}
