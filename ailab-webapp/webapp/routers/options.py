import traceback

from fastapi import APIRouter
from datetime import datetime, timedelta
from ..data.get_options import get_options_expiration_date, get_options_data
from models.Options.options import Options
from ..helpers import round_result
from models.Portfolio import Portfolio
from ..webapp_models.generic_models import ResultResponse, Data, CDSData
from ..CONSTS import ANALYTICS_DECIMALS
from ..webapp_models.generic_models import ResultResponse
import pandas as pd

router = APIRouter(
    prefix="/options",
    tags=["options"]
)


@router.get("/get_options_expiration_date/{ticker}")
def get_options_expiration_date_api(ticker):
    try:
        result = get_options_expiration_date(ticker)
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@router.post("/get_options_data_api")
def get_options_data_api(requestBody: dict):
    try:
        result = get_options_data(requestBody['ticker'], datetime.strptime(requestBody['date'], '%B %d, %Y'),
                                  requestBody['options_type'])
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@router.post("/options_pricing")
def options_pricing_api(request_body: dict):
    try:
        s, k, rf, div, vol, T, options_type, N, method = request_body['s'], request_body['k'], request_body['rf'], \
                                                         request_body['div'], request_body['vol'], request_body['T'], \
                                                         request_body['options_type'], request_body['N'], request_body[
                                                             'method']
        pc_flag = 1 if options_type == 'call' else -1
        options = Options(s, k, rf, div, vol, T, pc_flag)
        if method == 'BS':
            result = options.bs()
        elif method == 'Binomial Tree':
            result = options.binomial_tree(N)
        else:
            result = 0
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)