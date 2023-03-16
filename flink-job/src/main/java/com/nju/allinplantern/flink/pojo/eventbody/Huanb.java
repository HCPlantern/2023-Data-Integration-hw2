package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 贷款还本明细
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Huanb extends EventBody{
    private String tran_flag;

    private String uid;

    private String cust_name;

    private String acct_no;

    private String tran_date;

    private String tran_time;

    private BigDecimal tran_amt;

    private BigDecimal bal;

    private String tran_code;

    private String dr_cr_code;

    private Integer pay_term;

    private String tran_teller_no;

    private BigDecimal pprd_rfn_amt;

    private BigDecimal pprd_amotz_intr;

    private String tran_log_no;

    private String tran_type;

    private String dscrp_code;

    private String remark;

    private String etl_dt;

    @Override
    public boolean isValid() {
        return false;
    }
}
