import traceback

from fastapi import APIRouter

from .. import CONSTS
from ..helpers import round_result
from fermi_backend.models.Portfolio import Portfolio
from ..webapp_models.generic_models import ResultResponse, Data, CDSData
from ..CONSTS import ANALYTICS_DECIMALS
from ..webapp_models.generic_models import ResultResponse
import pandas as pd

router = APIRouter(
    prefix="/portfolio",
    tags=["portfolio"]
)


@router.post("/get_basic_info")
def get_basic_info(data: Data) -> ResultResponse:
    try:
        df = pd.DataFrame(data.data)
        p = Portfolio(df, weights=data.weights)
        basic_info = p.basic_info()
        basic_info[''] = basic_info.index
        cols = list(basic_info.columns)
        reorder = [cols[-1]] + cols[:-1]
        result = basic_info[reorder].to_json(orient='records')
    except Exception as e:
        return ResultResponse(status_code=CONSTS.HTTP_500_INTERNAL_SERVER_ERROR, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status_code=CONSTS.HTTP_200_OK, result=result)


@round_result(ANALYTICS_DECIMALS)
def get_all_var(data, weights, level, decay, n):
    p = Portfolio(data, weights)
    result = {'Historical VaR': p.hvar(level),
              'Parametric VaR': p.pvar(level / 100, alpha=decay),
              'Monte Carlo VaR': p.monte_carlo_var(level, n)}
    return result


@router.post("/valueatrisk")
def valueatrisk(requestbody: dict) -> ResultResponse:
    try:
        data, weights, level, decay, n = pd.DataFrame(requestbody['data']), requestbody['weights'], requestbody[
            'level'], requestbody['alpha'], requestbody['n']
        result = get_all_var(data, weights, level, decay, n)
    except Exception as e:
        return ResultResponse(status_code=CONSTS.HTTP_500_INTERNAL_SERVER_ERROR, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status_code=CONSTS.HTTP_200_OK, result=result)


@round_result(ANALYTICS_DECIMALS)
def weights_optimization(data, weights, expected_return):
    p = Portfolio(data, weights=weights)
    return list(p.optimization(given_return=expected_return))


@router.post("/weights_optimization")
def weights_optimization_api(requestbody: dict) -> ResultResponse:
    try:
        data, weights, expected_return = pd.DataFrame(requestbody['data']), requestbody['weights'], requestbody[
            'expected_return']
        expected_return = None if requestbody['expected_return'] == '' else float(expected_return)

        result = weights_optimization(data, weights, expected_return)
    except Exception as e:
        return ResultResponse(status_code=CONSTS.HTTP_500_INTERNAL_SERVER_ERROR, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status_code=CONSTS.HTTP_200_OK, result=result)


@router.post("/sharpe_ratio")
def sharpe_ratio(requestbody: dict) -> ResultResponse:
    try:
        df = pd.DataFrame(requestbody['data'])
        p = Portfolio(df, weights=requestbody['weights'])
        result = p.sharpe_r(requestbody['rf'])
    except Exception as e:
        return ResultResponse(status_code=CONSTS.HTTP_500_INTERNAL_SERVER_ERROR, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status_code=CONSTS.HTTP_200_OK, result=result)