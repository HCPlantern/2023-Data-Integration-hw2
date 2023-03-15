package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 水电燃气交易
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Sdrq extends EventBody {

    private String hosehld_no;

    private String acct_no;

    private String cust_name;

    private String tran_type;

    private String tran_date;

    private BigDecimal tran_amt_fen;

    private String channel_flg;

    private String tran_org;

    private String tran_teller_no;

    private String tran_log_no;

    private String batch_no;

    private String tran_sts;

    private String return_msg;

    private String etl_dt;

    private String uid;

    @Override
    public boolean isValid() {
        return false;
    }
}
