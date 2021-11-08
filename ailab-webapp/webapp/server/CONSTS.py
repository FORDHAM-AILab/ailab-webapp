import pathlib
import os


API_VERSION = '1.11'
PACKAGE_PATH = pathlib.Path().resolve()
ROOT_PATH = pathlib.Path.home()

if os.name == "nt":
    print(os.name)
    TEMP_PATH = PACKAGE_PATH
    EFS_PATH = None
else:
    EFS_PATH = ROOT_PATH.parent.parent.joinpath('mnt', 'efs_mtge')
    # EFS_PATH = ROOT_PATH.parent.parent.joinpath('home', 'gwu')
    TEMP_PATH = EFS_PATH.joinpath('tmp')

RESULT_S3_BUCKET = "pmc02-test"
S3_NPL_OUTDIR = "npl_output"
PRESIGNED_URL_LIFE = 7200
LOOKBACK_MONTHS_FOR_RECENT_DEALS = 4

# ECS settings
ECS_CLUSTER = "FargateCluster"
ECS_REGION = "us-east-1"
ECS_WORKER = "rcf-worker"
ECS_SERVER = "rcf-service"

NPL_BULK_OVERRIDE_FIELDS = [
    "portfolio_name", "txn_deal_name", "seller_name", "deal_name", "pool_name", "loan_id", "total_unpaid_bal_amt",
    "svcr_loan_id", "seller_loan_id", "asof_date", "prop_type_code", "prop_year_built", "prop_gla_sqft",
    "prop_bedrooms", "prop_baths", "prop_stories", "prop_pool_flag", "prop_flood_flag", "prop_sec8_flag",
    "occupancy_code", "prop_condition_code", "list_price", "bpo_as_is_value", "waterfall_value_amt",
    "reo_haircut_value_amt", "rental_haircut_value_amt", "loan_status_code", "approx_int_due", "corp_adv_amt",
    "escrow_adv_amt", "suspense_bal_amt", "defer_bal_amt", "defer_collect_rate", "curr_bal_amt", "prop_ins_amt",
    "total_debt_amt", "min_val_amt", "jr_lien_amt", "non_fc_lien_amt", "non_negotiable_lien_amt", "fc_time_dist",
    "reo_time_dist", "avg_fc_time", "avg_reo_time", "eviction_time", "progress_mths", "prop_tax_amt",
    "hoa_fees", "fc_exp_amt", "reo_exp_amt", "rental_fit_score", "rental_mthly_amt", "wloan_type",
    "wloan_divestment_time", "wloan_divestment_end_date", "mdl_sale_type", "modify_delay_mths",
    "modify_fb_time", "modify_bal_amt", "modify_deferred_bal_amt", "modify_int_rate", "modify_term_mths",
    "ssale_delay_mths", "fcreo_delay_mths", "price_adj_type", "price_adj_delay_mths", "price_adj_amt",
    "wloan_prob", "modify_prob", "ssale_prob", "tps_prob", "fcreo_prob", "fcrental_prob",
    "chg_prob", "mdl_disc_rate", "hpi_fcst", "modify_divestment_end_date", "hpi_date"]

NPL_BULK_OVERRIDE_FIELD_MAPPING = {
    'hoa_mthly_amt': 'hoa_fees',
}


NPL_LOAN_LEVEL_UPLOAD_FIELDS = [
    'loan_id', 'fcreo_npv_amt', 'fcrental_npv_amt', 'tps_npv_amt', 'wloan_npv_amt', 'ssale_npv_amt', 'modify_npv_amt',
    'mdl_npv_amt', 'fcreo_prob', 'fcrental_prob', 'tps_prob', 'ssale_prob', 'modify_prob', 'wloan_prob', 'chg_prob',
    'price_adj_type', 'price_adj_delay_mths', 'price_adj_amt', 'portfolio_name', 'pool_name', 'seller_name',
    'seller_loan_id', 'deal_name', 'waterfall_value_amt', 'mdl_pool_name', 'override_type_name', 'mdl_total_debt_amt',
    'total_debt_amt', 'remaining_fc_time', 'asof_date', 'defer_bal_amt', 'curr_bal_amt', 'curr_int_rate',
    'txn_deal_name', 'total_unpaid_bal_amt', 'corp_adv_amt', 'escrow_adv_amt', 'progress_mths', 'address', 'state',
    'city', 'zip_cd', 'prop_type_code', 'occupancy_code', 'loan_status_code', 'bk_status_code', 'approx_int_due',
    'suspense_bal_amt', 'pmi_rate', 'curr_fico_score', 'outcome_custom', 'outcome_collateral', 'outcome_reperform',
    'bpo_as_is_value', 'delinquent_mths', 'cash_flow_vel_3_mth', 'cash_flow_vel_6_mth', 'reo_haircut_value_amt',
    'hpi_fcst_level', 'reo_status_code', 'rental_haircut_value_amt', 'finance_name', 'mdl_disc_rate', 'svcr_loan_id',
    'rental_mthly_amt', 'rental_status_code', 'fc_status_code', 'lm_status_code', 'am_sub_status_code',
    'am_status_code', 'modify_divestment_end_date', 'modify_bal_amt', 'modify_deferred_bal_amt', 'modify_total_bal_amt',
    'modify_int_rate', 'modify_fb_time', 'modify_delay_mths', "prop_value_id", "mdl_override_type_name",

]

NPL_LOAN_LEVEL_FIELD_MAPPING = {
    'fcreo_npv': 'fcreo_npv_amt',
    'fcrental_npv': 'fcrental_npv_amt',
    'tps_npv': 'tps_npv_amt',
    'wloan_npv': 'wloan_npv_amt',
    'ssale_npv': 'ssale_npv_amt',
    'modify_npv': 'modify_npv_amt',
    'mdl_npv': 'mdl_npv_amt',
}

NPL_DB_MDL_OVERRIDE_TABLE = 'prod.mdl_input_override'
NPL_DB_MDL_RUN_PARAMS_TABLE = 'mdl.mdl_run'
NPL_DB_LOAN_LEVEL_NPV_TABLE = 'mdl.mdl_loan_level'
NPL_DB_MONTHLY_CF_TABLE = 'mdl.mdl_mly_loan_level'

ACCESS_TOKEN_EXPIRE_MINUTES = 60
ENCRYPT_ALGORITHM = "HS256"
ENCRYPT_SECRET_KEY = "15c517cde7fcbfc6b4272858b78071beea4ae49897067f986fc315dc24065ffa"
# LOGIN_USERS_DB = {
#     "guest": {
#         "username": "guest",
#         # "hashed_password": "$2b$12$qeB27ID8SGEQLptPAnXOWeaeXoJYzfFR.fFbCfCJPOUN6dxDfaxrC",
#         "hashed_password": '082bf40e068fb312a62450503b4c641c96829ca1',  # SHA-1 of the original password
#         "disabled": False,
#     }
# }

# S3_SHARED_FOLDERS = [
#     'pmc02-sftp', 'pmc02-test'
# ]

REPORT_FREQ_USER_ACTIVITY = 3600 * 4
# USER_ACTIVITY_DB_TABLE = 'mdl.mtgeapp_usage'
DB_UPLOAD_MAX_ROW_NUM = 2048
