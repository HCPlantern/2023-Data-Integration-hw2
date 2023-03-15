package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 活期交易
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Sa extends EventBody {
    private String uid;

    private String card_no;

    private String cust_name;

    private String acct_no;

    private Integer det_n;

    private String curr_type;

    private String tran_teller_no;

    private BigDecimal cr_amt;

    private BigDecimal bal;

    private BigDecimal tran_amt;

    private String tran_card_no;

    private String tran_type;

    private String tran_log_no;

    private BigDecimal dr_amt;

    private String open_org;

    private String dscrp_code;

    private String remark;

    private String tran_time;

    private String tran_date;

    private String sys_date;

    private String tran_code;

    private String remark_1;

    private String oppo_cust_name;

    private String agt_cert_type;

    private String agt_cust_name;

    private String channel_flag;

    private String oppo_acct_no;

    private String oppo_bank_no;

    private String src_dt;

    private String etl_dt;

    @Override
    public boolean isValid() {
        return false;
    }
}
