import traceback

from fastapi import APIRouter
from ..webapp_models.generic_models import ResultResponse

from models.Sentiment.Analysis import news_analyzer, txt_analyzer, txt_summation

router = APIRouter(
    prefix="/sentiment",
    tags=["sentiment"]
)


@router.post("/get_recent_news_sentiment")
async def get_recent_news_sentiment():
    try:
        result = news_analyzer()
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@router.get("/get_single_stock_news_sentiment/{ticker}")
async def get_single_stock_news_sentiment(ticker):
    try:
        result = news_analyzer(ticker)
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@router.get("/get_text_sentiment/{txt_file}")
async def get_text_sentiment(txt_file):
    try:
        result = txt_analyzer(txt_file)
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@router.get("/financial_summary/{txt_file}")
async def financial_summary(txt_file):
    try:
        result = txt_summation(txt_file)
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)
