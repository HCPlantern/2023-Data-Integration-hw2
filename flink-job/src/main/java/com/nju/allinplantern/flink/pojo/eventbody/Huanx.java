package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 贷款还息明细
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Huanx extends EventBody {
    private String tran_flag;

    private String uid;

    private String cust_name;

    private String acct_no;

    private String tran_date;

    private String tran_time;

    private BigDecimal tran_amt;

    private String cac_intc_pr;

    private String tran_code;

    private String dr_cr_code;

    private Integer pay_term;

    private String tran_teller_no;

    private String intc_strt_date;

    private String intc_end_date;

    private String intr;

    private String tran_log_no;

    private String tran_type;

    private String dscrp_code;

    private String etl_dt;

    @Override
    public boolean isValid() {
        return false;
    }
}
