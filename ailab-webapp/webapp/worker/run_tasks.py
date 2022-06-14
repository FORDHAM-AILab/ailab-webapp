from __future__ import absolute_import
# from worker.tasks import add, market
import sys
import time
import datetime
from dateutil.relativedelta import relativedelta
import secrets
import os
import datetime


if __name__ == '__main__':
    # test for async
    # argv = sys.argv[1:]

    # result = add.delay(float(argv[0]), float(argv[1]))
    # print(result)
    # time.sleep(10)
    # print(result.status, result.result)

    # test for portfolio calc but without async
    print(os.getenv("CACHE_DB"))
    market_date = datetime.date(2021, 4, 30)
    file_name = 'rpl_loan_tape_sample4_fb6c366c.csv'
    hpa = 0
    rate_shock = 0
    basis = 0
    investor = "GSE"

    # file_name_wo_suffix = os.path.splitext(file_name)[0]
    # output_file_name = f"mdlcrv_{file_name_wo_suffix}.xlsx"
    # input_file_name_with_path = str(CONSTS.TEMP_PATH.joinpath(file_name))
    # output_file_name_with_path = str(CONSTS.TEMP_PATH.joinpath(output_file_name))
    #
    # mtgecrv.utils.set_consts_mtgecrv({
    #     "HPA": hpa,
    #     "RATE_SHOCK": rate_shock,
    #     "BASIS": basis,
    #     "INVESTOR": investor
    # })
    #
    # try:
    #     mtgecrv.run.run_curves(run_date=market_date, input_file_name=input_file_name_with_path,
    #                            output_file=output_file_name_with_path,
    #                            sector='rpl')
    # except:
    #     raise RuntimeError
    #
    # result_file_name = f"pcg/rpl/{output_file_name}"
    # mtgeaws.s3.upload_file_to_bucket(CONSTS.RESULT_S3_BUCKET, output_file_name_with_path, result_file_name)
    # url = mtgeaws.s3.generate_presigned_url(CONSTS.RESULT_S3_BUCKET, result_file_name, CONSTS.PRESIGNED_URL_LIFE)
    # print(url)

    file_name_wo_suffix = os.path.splitext(file_name)[0]
    output_file_name = f"mdlcrv_{file_name_wo_suffix}.xlsx"
    input_file_name_with_path = str(CONSTS.TEMP_PATH.joinpath(file_name))
    output_file_name_with_path = str(CONSTS.TEMP_PATH.joinpath(output_file_name))

    mtgecrv.utils.set_consts_mtgecrv({
        "HPA": hpa,
        "RATE_SHOCK": rate_shock,
        "BASIS": basis,
        "INVESTOR": investor
    })
    try:
        mtgecrv.run.run_curves(run_date=market_date, input_file_name=input_file_name_with_path,
                               output_file=output_file_name_with_path,
                               sector='rpl')
    except:
        raise RuntimeError
    print("calculated")
    result_file_name = f"pcg/rpl/{output_file_name}"
    mtgeaws.s3.upload_file_to_bucket(CONSTS.RESULT_S3_BUCKET, output_file_name_with_path, result_file_name)
    url = mtgeaws.s3.generate_presigned_url(CONSTS.RESULT_S3_BUCKET, result_file_name, CONSTS.PRESIGNED_URL_LIFE)
    print(url)