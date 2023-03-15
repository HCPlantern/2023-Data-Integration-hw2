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
    // 是否都用 String ?
    private String tran_channel;

    private String order_code;

    private String shop_code;

    private String shop_name;

    private String hlw_tran_type;

    private String tran_date;

    private String tran_time;

    private BigDecimal tran_amount;

    private String current_status;

    private String score_num;

    private String pay_channel;

    private String uid;

    private String legal_name;

    private String etl_dt;

    @Override
    public boolean isValid() {
        return false;
    }
}
