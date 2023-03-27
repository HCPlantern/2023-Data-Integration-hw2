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
    cred_limit Nullable(decimal(18,2)),
    mob_phone String,
    deposit Nullable(decimal(18,2)),
    over_draft Nullable(decimal(18,2)),
    dlay_amt Nullable(decimal(18,2)),
    five_class String,
    bankacct String,
    bankacct_date String,
    bankacct_bal Nullable(decimal(18,2)),
    is_mob_bank String,
    mob_bank_date String,
    is_etc String,
    etc_date String,
    issue_mode String,
    issue_mode_name String,
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
    mp_number String,
    mp_type String,
    mp_status String,
    purch_date String,
    purch_mth String,
    total_amt Nullable(decimal(18,2)),
    total_mths int,
    mth_instl Nullable(decimal(18,2)),
    instl_cnt int,
    rem_ppl Nullable(decimal(18,2)),
    total_fee Nullable(decimal(18,2)),
    rem_fee Nullable(decimal(18,2)),
    rec_fee Nullable(decimal(18,2)),
    etl_dt String
)
ENGINE = MergeTree
ORDER BY acct_no;

CREATE TABLE dm.pri_cust_asset_acct_info (
    cust_no String,
    cust_name String,
    uid String,
    acct_no String,
    card_no String,
    curr_type String,
    subject_no String,
    prod_type String,
    belong_org String,
    exam_org String,
    open_org String,
    open_date String,
    open_teller_no String,
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
    acct_bal Nullable(decimal(18,2)),
    bal Nullable(decimal(18,2)),
    avg_mth Nullable(decimal(18,2)),
    avg_qur Nullable(decimal(18,2)),
    avg_year Nullable(decimal(18,2)),
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
    all_bal Nullable(decimal(18,2)),
    avg_mth Nullable(decimal(18,2)),
    avg_qur Nullable(decimal(18,2)),
    avg_year Nullable(decimal(18,2)),
    sa_bal Nullable(decimal(18,2)),
    td_bal Nullable(decimal(18,2)),
    fin_bal Nullable(decimal(18,2)),
    sa_crd_bal Nullable(decimal(18,2)),
    td_crd_bal Nullable(decimal(18,2)),
    sa_td_bal Nullable(decimal(18,2)),
    ntc_bal Nullable(decimal(18,2)),
    td_3m_bal Nullable(decimal(18,2)),
    td_6m_bal Nullable(decimal(18,2)),
    td_1y_bal Nullable(decimal(18,2)),
    td_2y_bal Nullable(decimal(18,2)),
    td_3y_bal Nullable(decimal(18,2)),
    td_5y_bal Nullable(decimal(18,2)),
    oth_td_bal Nullable(decimal(18,2)),
    cd_bal Nullable(decimal(18,2)),
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
    contact_phone String,
    contact_address String
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
    loan_amt Nullable(decimal(18,2)),
    loan_bal Nullable(decimal(18,2)),
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
    credit_amt Nullable(decimal(18,2)),
    credit_begin_date String,
    credit_matu_date String,
    frst_intr Nullable(decimal(18,4)),
    actu_intr Nullable(decimal(18,4)),
    loan_mob_phone String,
    loan_use String,
    inte_settle_type String,
    bankacct String,
    defect_type String,
    owed_int_in Nullable(decimal(18,2)),
    owed_int_out Nullable(decimal(18,2)),
    delay_bal Nullable(decimal(18,2)),
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
    guar_no String,
    guar_right_no String,
    guar_name String,
    guar_amount Nullable(decimal(18,2)),
    guar_add String,
    guar_eva_value Nullable(decimal(18,2)),
    guar_con_value Nullable(decimal(18,2)),
    guar_reg_date String,
    guar_matu_date String,
    etl_dt String
)
ENGINE = MergeTree
ORDER BY (loan_cust_no, acct_no);

CREATE TABLE dm.pri_cust_liab_info (
    uid String,
    all_bal Nullable(decimal(18,2)),
    bad_bal Nullable(decimal(18,2)),
    due_intr Nullable(decimal(18,2)),
    norm_bal Nullable(decimal(18,2)),
    delay_bal Nullable(decimal(18,2)),
    etl_dt String
)
ENGINE = MergeTree
ORDER BY uid;

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


