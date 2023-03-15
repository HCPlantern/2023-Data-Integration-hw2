package com.nju.allinplantern.flink.pojo.eventbody;

import java.math.BigDecimal;

public class Sjyh extends EventBody {
    private String uid;

    private String mch_channel;

    private String login_type;

    private String ebank_cust_no;

    private String tran_date;

    private String tran_time;

    private String tran_code;

    private String tran_sts;

    private String return_code;

    private String return_msg;

    private String sys_type;

    private String payer_acct_no;

    private String payer_acct_name;

    private String payee_acct_no;

    private String payee_acct_name;

    private BigDecimal tran_amt;

    private String etl_dt;

    @Override
    public boolean isValid() {
        return false;
    }
}
