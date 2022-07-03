import traceback
from typing import List

from fastapi import APIRouter, Query

from ..data.stock import get_hist_stock_price, get_single_hist_price, get_analysis_info
from ..webapp_models.generic_models import ResultResponse

router = APIRouter(
    prefix="/stock",
    tags=["stock"]
)


@router.get("/load_hist_stock_price/{start_date}/{end_date}")
async def load_hist_stock_price(start_date, end_date, q: List[str] = Query(None)):
    stock_price = get_hist_stock_price(tickers=q, start_date=start_date, end_date=end_date)
    return {i: stock_price[i].to_list() for i in stock_price.columns}


@router.get("/load_single_hist_stock_price/{ticker}/{start_date}/{end_date}")
async def load_full_hist_stock_price(ticker, start_date, end_date):
    try:
        result = get_single_hist_price(ticker, start_date, end_date)
        result = result.to_json(orient='records')
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@router.get("/get_analysis_info/{ticker}")
async def get_analysis_info_api(ticker):
    try:
        result = get_analysis_info(ticker)
        for k, v in result.items():
            result[k] = v.to_json(orient='records')
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)