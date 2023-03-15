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
    private String uid;

    private String cust_name;

    private String tran_date;

    private String tran_sts;

    private String tran_org;

    private String tran_teller_no;

    private BigDecimal tran_amt_fen;

    private String tran_type;

    private String return_msg;

    private String etl_dt;


    @Override
    public boolean isValid() {
        return false;
    }
}
