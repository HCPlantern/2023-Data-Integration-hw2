package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * ETC交易
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Etc extends EventBody {
    /**
     * 证件号码
     */
    private String uid;

    /**
     * ETC账号
     */
    private String etc_acct;

    /**
     * 卡号
     */
    private String card_no;

    /**
     * 车牌号
     */
    private String car_no;

    /**
     * 客户名称
     */
    private String cust_name;

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
    private BigDecimal tran_amt_fen;

    /**
     * 实收金额
     */
    private BigDecimal real_amt;

    /**
     * 优惠金额
     */
    private BigDecimal conces_amt;

    /**
     * 通行路程
     */
    private String tran_place;

    /**
     * 手机号码
     */
    private String mob_phone;

    /**
     * 数据日期
     */
    private String etl_dt;


    @Override
    public boolean isValid() {
        return false;
    }
}
