from __future__ import absolute_import
from .celery_app import celery_app
from celery import group, chain, chord
from time import sleep

import datetime
import json
import os
import pandas as pd
import traceback
from pandas.core.frame import DataFrame
from typing import Optional, List, Tuple
from pathlib import PurePath
from celery.contrib import rdb
from celery.utils.log import get_task_logger
import time


logger = get_task_logger(__name__)


@celery_app.task(name='tasks.add')
def add(x, y):
    print(x+y)
    return x+y


@celery_app.task(name='tasks.kwarg_add')
def add(param1: int, param2: int):
    print(param1, param2)
    return param1 + param2


@celery_app.task(name='tasks.add_list')
def add(x_args: List[int] = [], offset: int = 0):
    print(f"arguments passed are: {x_args}")
    print(f"offset passed is: {offset}")
    return sum(x_args) + offset


@celery_app.task(name='tasks.error_handler')
def on_chord_error(request, exc, traceback):
    print('calling chord error method!!!!')
    print(f'Task {request.id} raised error: {exc} and traceback is {traceback}')
    return


@celery_app.task(name='tasks.dummy_agg')
def dummy_agg(args, **kwargs):
    print(f'args are: {args} and kwargs are {kwargs}')
    return


@celery_app.task(name='tasks.xlist')
def x_list(param1, param2):
    return [x for x in range(param1, param2)]


@celery_app.task(name='tasks.xlist_sum')
def x_list_sum(args, param=1):
    result = []
    print(f"args are {args}")
    for elem in args:
        print(f"elem is {elem}")
        result += elem

    return sum(result) * param


@celery_app.task(name='tasks.internal_dist')
def internal_dist():
    job1 = celery_app.signature('tasks.xlist', args=[0, 5])
    job2 = celery_app.signature('tasks.xlist', args=[6, 10])
    jobs = group([job1, job2])


@celery_app.task(name='tasks.fail')
def fail(x: int, raise_exception: bool):
    time.sleep(x * 1)
    if x < 3:
        if raise_exception:
            raise Exception(f"x is equal to {x}")
        else:
            x = 1/0
    else:
        return x


@celery_app.task(name='tasks.mul')
def add(x, y):
    sleep(5)
    print(x*y)
    return x*y


@celery_app.task(name='tasks.err')
def err():
    raise ValueError('Exception encountered')


@celery_app.task(name='tasks.prog', bind=True)
def progress(self, param: int):
    i = 0
    while i < param:
        i += 1
        if i % (param/10) == 0:
            self.update_state(state='PROGRESS',
                              meta={'current': i, 'total': param})

    return True

#
#
# @celery_app.task(name='tasks.market')
# def market(market_date):
#     if market_date == "" or market_date is None:
#         market_date = datetime.date.today().isoformat()
#     date = datetime.date.fromisoformat(market_date)
#     results = rcf.model.market.market_rates(date)
#     return results.to_json(date_format="iso")
#
#
# @celery_app.task(name='tasks.portfolio_cf_multi')
# def portfolio_cf_multi(config, start_dates, cache_id_map):
#     logger.debug("reached portfolio_cf")
#     settings = json.loads(config, cls=mtgeutils.utils_decoder.JsonMtgeSimpleDecoder)
#     set_market = rcf.model.market.set_market_config(settings)
#     set_fund = rcf.model.fund_config.set_fund_config(settings)
#     results = rcf.model.portfolio.portfolio_cf_multi(start_dates=start_dates, cache_id_map=cache_id_map, write_cache=True)
#     return True
#
#
# @celery_app.task(name='tasks.defaults')
# def defaults(default_type: int):
#     if default_type == "inputs":
#         results = mtgeutils.utils_file.load_file('server/static', 'fundinputs.json')
#     elif default_type == "results":
#         results = mtgeutils.utils_file.load_file('server/static', 'fundresults.json')
#     else:
#         results = ""
#     print(results)
#     return results
#
#
# @celery_app.task(name='tasks.pcg_rpl')
# def run_rpl_curves(hpa: float, basis: float, rate_shock: float, investor: str, market_date: datetime.date,
#                    file_name: str, chunk_id: int, chunk_size: int):
#     logger.debug("entering pcg_rpl_{chunk_id} inside celery worker")
#     input_file_name, output_file_name, s3_file_name = utils_file.file_map(file_name, "mdlcrv", "pcg/rpl")
#
#     mtgecrv.utils.set_consts_mtgecrv({
#         "HPA": hpa,
#         "RATE_SHOCK": rate_shock,
#         "BASIS": basis,
#         "INVESTOR": investor
#     })
#     # TODO: figure out how datetime can be automatically be de-serialized
#     # market_date = datetime.datetime.fromisoformat(market_date).date()
#     try:
#         # uncomment below line if debugging
#         # rdb.set_trace()
#         results = mtgecrv.run.run_curves(run_date=market_date, input_file_name=input_file_name,
#                                sector='rpl', chunk_id=chunk_id, chunk_size=chunk_size)
#         logger.debug("completed results calculation inside pcg_rpl")
#         return results
#     except Exception as error:
#         raise RuntimeError('Error inside pcg/rpl') from error
#
#
# @celery_app.task(name='tasks.pcg_rpl_agg')
# def agg_rpl_results(result_list, **kwargs):
#     file_name = kwargs.get('file_name')
#     input_file_name, output_file_name, s3_file_name = utils_file.file_map(file_name, "mdlcrv", "pcg/rpl")
#     results = mtgecrv.run.aggregate_and_output_results(result_list, output_file=output_file_name, sector="rpl")
#     url = utils_file.copy_and_generate_url(output_file_name, s3_file_name)
#     return {"urls": [url], "results": results}
#
#
# @celery_app.task(name='tasks.pcg_nqm')
# def run_nqm_curves(cpr_mult: float, cdr_mult: float, sev_adj: float, custom_curves: str, market_date: datetime.date,
#                    file_name: str, chunk_id: int, chunk_size: int):
#     logger.debug("entering pcg_nqm inside celery worker")
#     input_file_name, output_file_name, s3_file_name = utils_file.file_map(file_name, "mdlcrv", "pcg/nqm")
#
#     mtgecrv.utils.set_consts_mtgecrv({
#         "CPR_MULT": cpr_mult/100,
#         "CDR_MULT": cdr_mult/100,
#         "SEV_ADJ": sev_adj/100,
#         "CUSTOM_CURVES": custom_curves
#     })
#
#     try:
#         # uncomment below line if debugging
#         # rdb.set_trace()
#         results = mtgecrv.run.run_curves(run_date=market_date, input_file_name=input_file_name,
#                                          sector='nqm', chunk_id=chunk_id, chunk_size=chunk_size)
#         logger.debug("completed results calculation inside pcg_nqm")
#         return results
#     except Exception as error:
#         raise RuntimeError('Error inside pcg/nqm')
#
#
# @celery_app.task(name='tasks.pcg_nqm_agg')
# def agg_nqm_results(result_list, **kwargs):
#     file_name = kwargs.get('file_name')
#     input_file_name, output_file_name, s3_file_name = utils_file.file_map(file_name, "mdlcrv", "pcg/nqm")
#     results = mtgecrv.run.aggregate_and_output_results(result_list, output_file=output_file_name, sector="nqm")
#     url = utils_file.copy_and_generate_url(output_file_name, s3_file_name)
#     return {"urls": [url], "results": results}
#
#
# @celery_app.task(name='tasks.npl_calc')
# def npl_calc(loan_data: str, outfile_path: Tuple[str], assumption: str, mdl_params: Optional[dict] = None,
#              price_date: Optional[str] = None) -> bool:
#     logger.debug("entering npl_calc inside celery worker")
#     try:
#         price_loans(loan_data, outfile_path, assumption, mdl_params, price_date)
#         logger.debug("finished npl_calc inside celery worker")
#     except Exception as error:
#         raise RuntimeError(f'Error occurred during NPL calculation:\n{traceback.format_exc()}')
#         return False
#     finally:
#         return True
#
#
# @celery_app.task(name='tasks.npl_agg_results')
# def npl_agg_results(infile_list: list, outfile_list: Tuple[str], input_info: NPLInputPassthrough) -> dict:
#     """
#     Aggregate model outputs, generate summary, and save results to S3
#     @param outfile_list: Save to path
#     @type outfile_list: string
#     @param infile_list:
#     @type infile_list:  List of strings (file paths)
#     @param input_info:  Pass through loan info
#     @type input_info:  int
#     @return: summary is a dictionary
#     @rtype:
#     """
#     logger.debug("entering aggregate job")
#     npv_outfile_name = outfile_list[0]
#     dfs = [pd.read_csv(f[0]) for f in infile_list]
#     loan_data = pd.concat(dfs, ignore_index=True)
#     loan_data.sort_values(by=['loan_id'], inplace=True)
#     npv_tempfile_path = os.path.join(CONSTS.TEMP_PATH, npv_outfile_name)
#     loan_data.to_csv(npv_tempfile_path, index=False)
#     s3_npv_file_name = f"{CONSTS.S3_NPL_OUTDIR}/{npv_outfile_name}"
#     s3_loan_level_url = utils_file.copy_and_generate_url(npv_tempfile_path, s3_npv_file_name)
#
#     mthly_cf_file_name = outfile_list[1]
#     dfs = [pd.read_csv(f[1]) for f in infile_list]
#     mthly_cf_data = pd.concat(dfs, ignore_index=True)
#     mthly_cf_data.sort_values(by=['loan_id', 'period'], inplace=True)
#     mthly_cf_tempfile_path = os.path.join(CONSTS.TEMP_PATH, mthly_cf_file_name)
#     mthly_cf_data.to_csv(mthly_cf_tempfile_path, index=False)
#     s3_cf_file_name = f"{CONSTS.S3_NPL_OUTDIR}/{mthly_cf_file_name}"
#     s3_mthly_cf_url = utils_file.copy_and_generate_url(mthly_cf_tempfile_path, s3_cf_file_name)
#     s3_params_file_name = f"{CONSTS.S3_NPL_OUTDIR}/{input_info.mdl_param_file_path.split('/')[-1]}"
#     s3_params_url = utils_file.copy_and_generate_url(input_info.mdl_param_file_path, s3_params_file_name)
#
#     loan_summary = gen_summary_outputs(loan_data, input_info.rate_shocks)
#     loan_summary["# of loans failed"] = input_info.loan_count - loan_summary["# of loans"]
#     results = {
#         "summary": loan_summary,
#         "loan-level file url": s3_loan_level_url,
#         "monthly cf file url": s3_mthly_cf_url,
#         "params file url": s3_params_url,
#         "error log file url": None,
#         "loan-level file path": npv_tempfile_path,
#         "monthly cf file path": mthly_cf_tempfile_path,
#         "loan info": input_info.loan_info,
#         "mdl_params file path": input_info.mdl_param_file_path,
#     }
#     if input_info.raw_data_path is not None:
#         raw_data_file_name = f"{CONSTS.S3_NPL_OUTDIR}/{input_info.raw_data_path.split('/')[-1]}"
#         s3_raw_data_url = utils_file.copy_and_generate_url(input_info.raw_data_path, raw_data_file_name)
#         results['raw data file url'] = s3_raw_data_url
#     if loan_summary["# of loans failed"] > 0:
#         error_file_name = outfile_list[2]
#         error_list = [pd.read_csv(f[2]) for f in infile_list if os.path.isfile(f[2])]
#         error_df = pd.concat(error_list, ignore_index=True)
#         error_df.sort_values(by=['loan_id'], inplace=True)
#         error_tempfile_path = os.path.join(CONSTS.TEMP_PATH, error_file_name)
#         error_df.to_csv(error_tempfile_path, index=False)
#         s3_error_file_name = f"{CONSTS.S3_NPL_OUTDIR}/{error_file_name}"
#         s3_error_file_url = utils_file.copy_and_generate_url(error_tempfile_path, s3_error_file_name)
#         results["error log file url"] = s3_error_file_url
#
#     logger.debug(f"exit agg job {results}")
#     return results
#
#
# @celery_app.task(name='tasks.pull_hpi')
# def pull_hpi(zip_code: str, con_id: int) -> dict:
#     sql_query = f"select hpi_fcst from prod.zip where zip_cd = {zip_code}"
#     rds.conn_pool.dispose()
#     db_rtn = rds.conn_pool.query_pd(sql_query=sql_query)
#     if db_rtn is None:
#         print("Shit! DB failed")
#         return -1
#     else:
#         print("DB is responding")
#         return 0

