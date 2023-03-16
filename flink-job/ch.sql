CREATE TABLE dm.dm_hlw_shop_info (
    shop_code String,
    shop_name String,
    shop_reg_date String,
    shop_add String,
    is_dafeng String,
    shop_type String,
    user_code String,
    band_card_no String,
    band_card_date String,
    is_inter_card String,
    is_cloud_flash String,
    card_bal decimal(18,2),
    card_avg_year decimal(18,2),
    card_avg_mth decimal(18,2),
    all_dep_bal decimal(18,2),
    legal_name String,
    uid String,
    legal_mob_phone String,
    shop_sts String,
    del_flag String,
    engage_type String,
    recom_no String,
    recom_name String,
    recom_org String,
    recom_org_name String,
    recom_phone String,
    tran_cnt_year int,
    tran_amt_year decimal(18,2),
    tran_cnt_mth int,
    tran_amt_mth decimal(18,2),
    class_year String,
    class_mth String,
    online_loan_bal decimal(18,2),
    all_loan_bal decimal(18,2),
    etl_dt String
)
ENGINE = MergeTree
ORDER BY shop_code;


CREATE TABLE dm.dm_v_as_djk_info (
    acct_no String,
    card_no String,
    cust_name String,
    prod_code String,
    prod_name String,
    uid String,
    entp_name String,
    open_date String,
    card_sts String,
    card_sts_name String,
    card_sts_date String,
    is_withdrw String,
    is_transfer String,
    is_deposit String,
    is_purchse String,
    cred_limit decimal(18,2),
    mob_phone String,
    deposit decimal(18,2),
    over_draft decimal(18,2),
    dlay_amt decimal(18,2),
    five_class String,
    bankacct String,
    bankacct_date String,
    bankacct_bal decimal(18,2),
    is_mob_bank String,
    mob_bank_date String,
    is_etc String,
    etc_date String,
    issue_mode String,
    issue_mode_name String,
    bal decimal(18,2),
    active_date String,
    clsd_date String,
    dlay_mths int,
    mgr_no String,
    mgr_name String,
    recom_name String,
    mge_org String,
    mge_org_name String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY acct_no;

CREATE TABLE dm.dm_v_as_djkfq_info (
    acct_no String,
    card_no String,
    cust_name String,
    uid String,
    mob_phone String,
    mge_org String,
    recom_no String,
    mp_number String,
    mp_type String,
    mp_status String,
    purch_date String,
    purch_mth String,
    total_amt decimal(18,2),
    total_mths int,
    mth_instl decimal(18,2),
    instl_cnt int,
    rem_ppl decimal(18,2),
    total_fee decimal(18,2),
    rem_fee decimal(18,2),
    rec_fee decimal(18,2),
    etl_dt String
)
ENGINE = MergeTree
ORDER BY acct_no;

CREATE TABLE dm.dm_v_as_etc_info (
    belong_org String,
    etc_acct String,
    etc_acct_type String,
    band_acct String,
    band_acct_owner String,
    cust_name String,
    uid String,
    sign_date String,
    mob_phone String,
    car_no String,
    car_model String,
    car_color String,
    buy_time String,
    car_sts String,
    seats String,
    car_type String,
    vehicle_type String,
    owner_name String,
    owner_cert_no String,
    owner_phone String,
    book_channel String,
    book_org String,
    book_emp String,
    book_emp_name String,
    cust_sts String,
    is_clsd_his String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY belong_org;

CREATE TABLE dm.dm_v_as_grwy_info (
    belong_org String,
    ebank_cust_no String,
    cust_name String,
    uid String,
    acct_no String,
    open_date String,
    open_teller_no String,
    mob_phone String,
    status String,
    last_tran_dt String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY belong_org;

CREATE TABLE dm.dm_v_as_sbdz_info (
    uid String,
    cust_name String,
    sign_date String,
    recom_no String,
    recom_name String,
    belong_org String,
    bel_org_name String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE dm.dm_v_as_sbyb_info (
    uid String,
    cust_name String,
    acct_no String,
    acct_name String,
    belong_org String,
    sign_date String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE dm.dm_v_as_sdm_info (
    type String,
    exam_org String,
    household_no String,
    sign_date String,
    acct_no String,
    cust_name String,
    uid String,
    is_secu_card String,
    sign_teller_no String,
    recom_no String,
    belong_org String,
    book_channel String,
    sign_channel String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY type;

CREATE TABLE dm.dm_v_as_sjhmzf_info (
    belong_org String,
    cust_name String,
    uid String,
    mob_phone String,
    card_no String,
    recom_no String,
    recom_name String,
    sign_date String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY belong_org;

CREATE TABLE dm.dm_v_as_sjyh_info (
    belong_org String,
    ebank_cust_no String,
    cust_name String,
    uid String,
    acct_no String,
    open_date String,
    reg_flag String,
    reg_flag_name String,
    open_teller_no String,
    recom_no String,
    recom_org String,
    mob_phone String,
    user_seq String,
    status String,
    is_djk String,
    is_sbk String,
    interbank_lmtamt decimal(18,2),
    innerbank_lmtamt decimal(18,2),
    tran_lmtamt decimal(18,2),
    last_tran_dt String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY belong_org;

CREATE TABLE dm.dm_v_as_wxyh_info (
    wechat_no String,
    acct_no String,
    acct_type String,
    uid String,
    band_sts String,
    band_date String,
    cust_no String,
    cust_name String,
    belong_org String,
    mob_phone String,
    email String,
    address String,
    notice_flag String,
    recom_no String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY wechat_no;

CREATE TABLE dm.dm_v_as_ybdzpz_info (
    uid String,
    ebank_cust_no String,
    sign_date String,
    recom_no String,
    recom_name String,
    belong_org String,
    bel_org_name String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE dm.dm_v_as_ydzf_info (
    type String,
    type_name String,
    belong_org String,
    acct_no String,
    is_secu_card String,
    cust_name String,
    uid String,
    sign_date String,
    sign_time String,
    sign_status String,
    cancel_date String,
    cancel_time String,
    recom_channel String,
    recom_org String,
    recom_no String,
    mob_phone String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY type;

CREATE TABLE dm.dm_v_tr_contract_mx (
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
    buss_amt decimal(18,2),
    loan_pert decimal(18,2),
    term_year int,
    term_mth int,
    term_day int,
    base_rate_type String,
    base_rate decimal(18,6),
    float_type String,
    rate_float decimal(18,6),
    rate decimal(18,6),
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
    actu_out_amt decimal(18,2),
    bal decimal(18,2),
    norm_bal decimal(18,2),
    dlay_bal decimal(18,2),
    dull_bal decimal(18,2),
    owed_int_in decimal(18,2),
    owed_int_out decimal(18,2),
    fine_pr_int decimal(18,2),
    fine_intr_int decimal(18,2),
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
    frz_amt decimal(18,0),
    con_crl_type String,
    shift_type String,
    due_intr_days int,
    reson_type String,
    shift_bal decimal(18,0),
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

CREATE TABLE dm.dm_v_tr_djk_mx (
    uid String,
    card_no String,
    tran_type String,
    tran_type_desc String,
    tran_amt decimal(18,2),
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

CREATE TABLE dm.dm_v_tr_dsf_mx (
    tran_date String,
    tran_log_no String,
    tran_code String,
    channel_flg String,
    tran_org String,
    tran_teller_no String,
    dc_flag String,
    tran_amt decimal(18,2),
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

CREATE TABLE dm.dm_v_tr_duebill_mx (
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
    buss_amt decimal(18,2),
    putout_date String,
    matu_date String,
    actu_matu_date String,
    buss_rate decimal(31,10),
    actu_buss_rate decimal(31,10),
    intr_type String,
    intr_cyc String,
    pay_times int,
    pay_cyc String,
    extend_times int,
    bal decimal(18,2),
    norm_bal decimal(18,2),
    dlay_amt decimal(18,2),
    dull_amt decimal(31,10),
    bad_debt_amt decimal(31,10),
    owed_int_in decimal(18,2),
    owed_int_out decimal(18,2),
    fine_pr_int decimal(18,2),
    fine_intr_int decimal(18,2),
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

CREATE TABLE dm.dm_v_tr_etc_mx (
    uid String,
    etc_acct String,
    card_no String,
    car_no String,
    cust_name String,
    tran_date String,
    tran_time String,
    tran_amt_fen decimal(18,2),
    real_amt decimal(18,2),
    conces_amt decimal(18,2),
    tran_place String,
    mob_phone String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE dm.dm_v_tr_grwy_mx (
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
    tran_amt decimal(18,2),
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE dm.dm_v_tr_gzdf_mx (
    belong_org String,
    ent_acct String,
    ent_name String,
    eng_cert_no String,
    acct_no String,
    cust_name String,
    uid String,
    tran_date String,
    tran_amt decimal(18,2),
    tran_log_no String,
    is_secu_card String,
    trna_channel String,
    batch_no String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY belong_org;

CREATE TABLE dm.dm_v_tr_huanb_mx (
    tran_flag String,
    uid String,
    cust_name String,
    acct_no String,
    tran_date String,
    tran_time String,
    tran_amt decimal(18,2),
    bal decimal(18,2),
    tran_code String,
    dr_cr_code String,
    pay_term int,
    tran_teller_no String,
    pprd_rfn_amt decimal(18,2),
    pprd_amotz_intr decimal(18,2),
    tran_log_no String,
    tran_type String,
    dscrp_code String,
    remark String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY tran_flag;

CREATE TABLE dm.dm_v_tr_huanx_mx (
    tran_flag String,
    uid String,
    cust_name String,
    acct_no String,
    tran_date String,
    tran_time String,
    tran_amt decimal(18,2),
    cac_intc_pr decimal(18,2),
    tran_code String,
    dr_cr_code String,
    pay_term int,
    tran_teller_no String,
    intc_strt_date String,
    intc_end_date String,
    intr decimal(18,2),
    tran_log_no String,
    tran_type String,
    dscrp_code String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY tran_flag;

CREATE TABLE dm.dm_v_tr_sa_mx (
    uid String,
    card_no String,
    cust_name String,
    acct_no String,
    det_n int,
    curr_type String,
    tran_teller_no String,
    cr_amt decimal(18,2),
    bal decimal(18,2),
    tran_amt decimal(18,2),
    tran_card_no String,
    tran_type String,
    tran_log_no String,
    dr_amt decimal(18,2),
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

CREATE TABLE dm.dm_v_tr_sbyb_mx (
    uid String,
    cust_name String,
    tran_date String,
    tran_sts String,
    tran_org String,
    tran_teller_no String,
    tran_amt_fen decimal(18,2),
    tran_type String,
    return_msg String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE dm.dm_v_tr_sdrq_mx (
    hosehld_no String,
    acct_no String,
    cust_name String,
    tran_type String,
    tran_date String,
    tran_amt_fen decimal(18,2),
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

CREATE TABLE dm.dm_v_tr_sjyh_mx (
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
    tran_amt decimal(18,2),
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE dm.pri_cust_asset_acct_info (
    cust_no String,
    cust_name String,
    uid String,
    acct_no String,
    card_no String,
    curr_type String,
    subject_no String,
    prod_type String,
    term String,
    rate decimal(18,0),
    auto_dp_flg String,
    belong_org String,
    exam_org String,
    open_org String,
    open_date String,
    open_teller_no String,
    matu_date String,
    acct_char String,
    deps_type String,
    prod_code String,
    clsd_org String,
    clsd_date String,
    clsd_teller_no String,
    is_secu_card String,
    acct_sts String,
    frz_sts String,
    stp_sts String,
    acct_bal decimal(18,2),
    bal decimal(18,2),
    avg_mth decimal(18,2),
    avg_qur decimal(18,2),
    avg_year decimal(18,2),
    etl_dt String
)
ENGINE = MergeTree
ORDER BY cust_no;

CREATE TABLE dm.pri_cust_asset_info (
    cust_no String,
    cust_name String,
    uid String,
    belong_org String,
    exam_org String,
    all_bal decimal(18,2),
    avg_mth decimal(18,2),
    avg_qur decimal(18,2),
    avg_year decimal(18,2),
    sa_bal decimal(18,2),
    td_bal decimal(18,2),
    fin_bal decimal(18,2),
    sa_crd_bal decimal(18,2),
    td_crd_bal decimal(18,2),
    sa_td_bal decimal(18,2),
    ntc_bal decimal(18,2),
    td_3m_bal decimal(18,2),
    td_6m_bal decimal(18,2),
    td_1y_bal decimal(18,2),
    td_2y_bal decimal(18,2),
    td_3y_bal decimal(18,2),
    td_5y_bal decimal(18,2),
    oth_td_bal decimal(18,2),
    cd_bal decimal(18,2),
    etl_dt String
)
ENGINE = MergeTree
ORDER BY cust_no;

CREATE TABLE dm.pri_cust_base_info (
    uid String,
    cert_type String,
    cust_no String,
    cust_name String,
    eng_name String,
    sex String,
    birthday String,
    cer_expd_date String,
    marrige String,
    education String,
    home_phone String,
    mob_phone String,
    oth_phone String,
    home_add String,
    reg_add String,
    work_unit String,
    work_unit_char String,
    "position" String,
    career String,
    prof_titl String,
    industry String,
    degree String,
    country String,
    nationality String,
    political_sts String,
    native_place String,
    province String,
    city String,
    conty String,
    town String,
    village String,
    village_group String,
    community String,
    is_employee String,
    is_shareholder String,
    is_black String,
    is_contact String,
    is_nine String,
    health_sts String,
    bad_habit String,
    mgr_name String,
    mgr_no String,
    mge_org_name String,
    mge_org String,
    create_date String,
    open_org String,
    open_teller_no String,
    update_date String,
    update_org String,
    update_teller_no String,
    etl_dt String,
    is_mgr_dep String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE dm.pri_cust_contact_info (
    uid String,
    con_type String,
    contact String,
    sys_source String,
    create_date String,
    update_date String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE dm.pri_cust_liab_acct_info (
    belong_org String,
    exam_org String,
    cust_no String,
    loan_cust_no String,
    cust_name String,
    uid String,
    acct_no String,
    begin_date String,
    matu_date String,
    settle_date String,
    subject_no String,
    prod_type String,
    buss_type String,
    buss_type_name String,
    loan_type String,
    float_tpename String,
    loan_amt decimal(18,2),
    loan_bal decimal(18,2),
    loan_mgr_no String,
    loan_mgr_name String,
    mgr_phone String,
    vouch_type String,
    putout_channel String,
    next_repay_date String,
    is_mortgage String,
    is_online String,
    is_extend String,
    extend_date String,
    ext_matu_date String,
    repay_type String,
    term_mth String,
    five_class String,
    overdue_class String,
    overdue_flag String,
    owed_int_flag String,
    contract_no String,
    credit_amt decimal(18,2),
    credit_begin_date String,
    credit_matu_date String,
    frst_intr decimal(18,4),
    actu_intr decimal(18,4),
    loan_mob_phone String,
    loan_use String,
    inte_settle_type String,
    bankacct String,
    defect_type String,
    owed_int_in decimal(18,2),
    owed_int_out decimal(18,2),
    delay_bal decimal(18,2),
    industr_type String,
    industr_type_name String,
    acct_sts String,
    arti_ctrt_no String,
    ext_ctrt_no String,
    flst_teller_no String,
    attract_no String,
    attract_name String,
    loan_use_add String,
    putout_acct String,
    is_book_acct String,
    book_acct_buss String,
    book_acct_amt String,
    sub_buss_type String,
    pro_char String,
    pro_char_ori String,
    pay_type String,
    grntr_name String,
    grntr_cert_no String,
    guar_no String,
    guar_right_no String,
    guar_name String,
    guar_amount decimal(18,2),
    guar_add String,
    guar_eva_value decimal(18,2),
    guar_con_value decimal(18,2),
    guar_reg_date String,
    guar_matu_date String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY (loan_cust_no, acct_no);

CREATE TABLE dm.pri_cust_liab_info (
    uid String,
    all_bal decimal(18,2),
    bad_bal decimal(18,2),
    due_intr decimal(18,2),
    norm_bal decimal(18,2),
    delay_bal decimal(18,2),
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE dm.v_tr_shop_mx (
    tran_channel String,
    order_code String,
    shop_code String,
    shop_name String,
    hlw_tran_type String,
    tran_date String,
    tran_time String,
    tran_amt decimal(18,2),
    current_status String,
    score_num decimal(18,2),
    pay_channel String,
    uid String,
    legal_name String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY tran_channel;

CREATE TABLE dm.pri_credit_info (
    uid String,
    credit_level String
)
ENGINE = MergeTree
ORDER BY uid;

CREATE TABLE dm.pri_star_info (
    uid String,
    star_level String
)
ENGINE = MergeTree
ORDER BY uid;


