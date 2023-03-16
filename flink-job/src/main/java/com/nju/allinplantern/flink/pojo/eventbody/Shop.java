package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 商户交易明细
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Shop extends EventBody {
    /**
     * 交易渠道
     */
    private String tran_channel;

    /**
     * 订单号
     */
    private String order_code;

    /**
     * 商户码
     */
    private String shop_code;

    /**
     * 商户名称
     */
    private String shop_name;

    /**
     * 交易类型
     */
    private String hlw_tran_type;

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
     * 交易状态
     */
    private String current_status;

    /**
     * 优惠积分
     */
    private BigDecimal score_num;

    /**
     * 支付渠道
     */
    private String pay_channel;

    /**
     * 负责人证件号码
     */
    private String uid;

    /**
     * 负责人名称
     */
    private String legal_name;

    /**
     * 数据跑批日期
     */
    private String etl_dt;


    @Override
    public boolean isValid() {
        return false;
    }
}
