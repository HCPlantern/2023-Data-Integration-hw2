DROP DATABASE if exists stream_test;
CREATE DATABASE stream_test;

CREATE TABLE stream_test.dm_v_tr_contract_mx (
    uid String,
    contract_no String,
    apply_no String,
    artificial_no String,
    occur_date String,
    loan_cust_no String,
    cust_name String,
    buss_type String,
    occur_type String,
    is_credit_cyc String,
    curr_type String,
    buss_amt Nullable(decimal(18,2)),
    loan_pert Nullable(decimal(18,2)),
    term_year int,
    term_mth int,
    term_day int,
    base_rate_type String,
    base_rate Nullable(decimal(18,6)),
    float_type String,
    rate_float Nullable(decimal(18,6)),
    rate Nullable(decimal(18,6)),
    pay_times int,
    pay_type String,
    direction String,
    loan_use String,
    pay_source String,
    putout_date String,
    matu_date String,
    vouch_type String,
    apply_type String,
    extend_times int,
    actu_out_amt Nullable(decimal(18,2)),
    bal Nullable(decimal(18,2)),
    norm_bal Nullable(decimal(18,2)),
    dlay_bal Nullable(decimal(18,2)),
    dull_bal Nullable(decimal(18,2)),
    owed_int_in Nullable(decimal(18,2)),
    owed_int_out Nullable(decimal(18,2)),
    fine_pr_int Nullable(decimal(18,2)),
    fine_intr_int Nullable(decimal(18,2)),
    dlay_days int,
    five_class String,
    class_date String,
    mge_org String,
    mgr_no String,
    operate_org String,
    operator String,
    operate_date String,
    reg_org String,
    register String,
    reg_date String,
    inte_settle_type String,
    is_bad String,
    frz_amt Nullable(decimal(18,0)),
    con_crl_type String,
    shift_type String,
    due_intr_days int,
    reson_type String,
    shift_bal Nullable(decimal(18,0)),
    is_vc_vouch String,
    loan_use_add String,
    finsh_type String,
    finsh_date String,
    sts_flag String,
    src_dt String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE stream_test.dm_v_tr_djk_mx (
    uid String,
    card_no String,
    tran_type String,
    tran_type_desc String,
    tran_amt Nullable(decimal(18,2)),
    tran_amt_sign String,
    mer_type String,
    mer_code String,
    rev_ind String,
    tran_desc String,
    tran_date String,
    val_date String,
    pur_date String,
    tran_time String,
    acct_no String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE stream_test.dm_v_tr_dsf_mx (
    tran_date String,
    tran_log_no String,
    tran_code String,
    channel_flg String,
    tran_org String,
    tran_teller_no String,
    dc_flag String,
    tran_amt Nullable(decimal(18,2)),
    send_bank String,
    payer_open_bank String,
    payer_acct_no String,
    payer_name String,
    payee_open_bank String,
    payee_acct_no String,
    payee_name String,
    tran_sts String,
    busi_type String,
    busi_sub_type String,
    etl_dt String,
    uid String
)
ENGINE = MergeTree
ORDER BY tran_date;

CREATE TABLE stream_test.dm_v_tr_duebill_mx (
    uid String,
    acct_no String,
    receipt_no String,
    contract_no String,
    subject_no String,
    cust_no String,
    loan_cust_no String,
    cust_name String,
    buss_type String,
    curr_type String,
    buss_amt Nullable(decimal(18,2)),
    putout_date String,
    matu_date String,
    actu_matu_date String,
    buss_rate Nullable(decimal(31,10)),
    actu_buss_rate Nullable(decimal(31,10)),
    intr_type String,
    intr_cyc String,
    pay_times int,
    pay_cyc String,
    extend_times int,
    bal Nullable(decimal(18,2)),
    norm_bal Nullable(decimal(18,2)),
    dlay_amt Nullable(decimal(18,2)),
    dull_amt Nullable(decimal(31,10)),
    bad_debt_amt Nullable(decimal(31,10)),
    owed_int_in Nullable(decimal(18,2)),
    owed_int_out Nullable(decimal(18,2)),
    fine_pr_int Nullable(decimal(18,2)),
    fine_intr_int Nullable(decimal(18,2)),
    dlay_days int,
    pay_acct String,
    putout_acct String,
    pay_back_acct String,
    due_intr_days int,
    operate_org String,
    operator String,
    reg_org String,
    register String,
    occur_date String,
    loan_use String,
    pay_type String,
    pay_freq String,
    vouch_type String,
    mgr_no String,
    mge_org String,
    loan_channel String,
    ten_class String,
    src_dt String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE stream_test.dm_v_tr_etc_mx (
    uid String,
    etc_acct String,
    card_no String,
    car_no String,
    cust_name String,
    tran_date String,
    tran_time String,
    tran_amt_fen Nullable(decimal(18,2)),
    real_amt Nullable(decimal(18,2)),
    conces_amt Nullable(decimal(18,2)),
    tran_place String,
    mob_phone String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE stream_test.dm_v_tr_grwy_mx (
    uid String,
    mch_channel String,
    login_type String,
    ebank_cust_no String,
    tran_date String,
    tran_time String,
    tran_code String,
    tran_sts String,
    return_code String,
    return_msg String,
    sys_type String,
    payer_acct_no String,
    payer_acct_name String,
    payee_acct_no String,
    payee_acct_name String,
    tran_amt Nullable(decimal(18,2)),
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE stream_test.dm_v_tr_gzdf_mx (
    belong_org String,
    ent_acct String,
    ent_name String,
    eng_cert_no String,
    acct_no String,
    cust_name String,
    uid String,
    tran_date String,
    tran_amt Nullable(decimal(18,2)),
    tran_log_no String,
    is_secu_card String,
    trna_channel String,
    batch_no String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY belong_org;

CREATE TABLE stream_test.dm_v_tr_huanb_mx (
    tran_flag String,
    uid String,
    cust_name String,
    acct_no String,
    tran_date String,
    tran_time String,
    tran_amt Nullable(decimal(18,2)),
    bal Nullable(decimal(18,2)),
    tran_code String,
    dr_cr_code String,
    pay_term int,
    tran_teller_no String,
    pprd_rfn_amt Nullable(decimal(18,2)),
    pprd_amotz_intr Nullable(decimal(18,2)),
    tran_log_no String,
    tran_type String,
    dscrp_code String,
    remark String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY tran_flag;

CREATE TABLE stream_test.dm_v_tr_huanx_mx (
    tran_flag String,
    uid String,
    cust_name String,
    acct_no String,
    tran_date String,
    tran_time String,
    tran_amt Nullable(decimal(18,2)),
    cac_intc_pr Nullable(decimal(18,2)),
    tran_code String,
    dr_cr_code String,
    pay_term int,
    tran_teller_no String,
    intc_strt_date String,
    intc_end_date String,
    intr Nullable(decimal(18,2)),
    tran_log_no String,
    tran_type String,
    dscrp_code String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY tran_flag;

CREATE TABLE stream_test.dm_v_tr_sa_mx (
    uid String,
    card_no String,
    cust_name String,
    acct_no String,
    det_n int,
    curr_type String,
    tran_teller_no String,
    cr_amt Nullable(decimal(18,2)),
    bal Nullable(decimal(18,2)),
    tran_amt Nullable(decimal(18,2)),
    tran_card_no String,
    tran_type String,
    tran_log_no String,
    dr_amt Nullable(decimal(18,2)),
    open_org String,
    dscrp_code String,
    remark String,
    tran_time String,
    tran_date String,
    sys_date String,
    tran_code String,
    remark_1 String,
    oppo_cust_name String,
    agt_cert_type String,
    agt_cert_no String,
    agt_cust_name String,
    channel_flag String,
    oppo_acct_no String,
    oppo_bank_no String,
    src_dt String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid
PARTITION BY tran_date;

CREATE TABLE stream_test.dm_v_tr_sbyb_mx (
    uid String,
    cust_name String,
    tran_date String,
    tran_sts String,
    tran_org String,
    tran_teller_no String,
    tran_amt_fen Nullable(decimal(18,2)),
    tran_type String,
    return_msg String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE stream_test.dm_v_tr_sdrq_mx (
    hosehld_no String,
    acct_no String,
    cust_name String,
    tran_type String,
    tran_date String,
    tran_amt_fen Nullable(decimal(18,2)),
    channel_flg String,
    tran_org String,
    tran_teller_no String,
    tran_log_no String,
    batch_no String,
    tran_sts String,
    return_msg String,
    etl_dt String,
    uid String
)
ENGINE = MergeTree
ORDER BY hosehld_no;

CREATE TABLE stream_test.dm_v_tr_shop_mx (
    tran_channel String,
    order_code String,
    shop_code String,
    shop_name String,
    hlw_tran_type String,
    tran_date String,
    tran_time String,
    tran_amt Nullable(decimal(18,2)),
    current_status String,
    score_num Nullable(decimal(18,2)),
    pay_channel String,
    uid String,
    legal_name String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY tran_channel;

CREATE TABLE stream_test.dm_v_tr_sjyh_mx (
    uid String,
    mch_channel String,
    login_type String,
    ebank_cust_no String,
    tran_date String,
    tran_time String,
    tran_code String,
    tran_sts String,
    return_code String,
    return_msg String,
    sys_type String,
    payer_acct_no String,
    payer_acct_name String,
    payee_acct_no String,
    payee_acct_name String,
    tran_amt Nullable(decimal(18,2)),
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;
