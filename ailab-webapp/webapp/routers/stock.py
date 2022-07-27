import traceback
from typing import List

from fastapi import APIRouter, Query

from ..data.stock import get_single_hist_price, get_analysis_info
from ..webapp_models.generic_models import ResultResponse

router = APIRouter(
    prefix="/stock",
    tags=["stock"]
)



@router.get("/get_analysis_info/{ticker}")
async def get_analysis_info_api(ticker):
    try:
        result = get_analysis_info(ticker)
        for k, v in result.items():
            result[k] = v.to_json(orient='records')
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)