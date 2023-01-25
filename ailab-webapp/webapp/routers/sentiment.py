import traceback

from fastapi import APIRouter
from ..webapp_models.generic_models import ResultResponse

from models.Sentiment.Analysis import news_analyzer, txt_analyzer, txt_summation, tweets_analyzer, reddits_analyzer
from models.Sentiment.DataScraping import convert_twitter_search

router = APIRouter(
    prefix="/sentiment",
    tags=["sentiment"]
)


@router.post("/get_recent_news_sentiment")
async def get_recent_news_sentiment():
    try:
        result = news_analyzer()
        for k, v in result.items():
            result[k] = v.to_json(orient='records')
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@router.get("/get_single_stock_news_sentiment/{ticker}")
async def get_single_stock_news_sentiment(ticker):
    try:
        result = news_analyzer(ticker)
        for k, v in result.items():
            result[k] = v.to_json(orient='records')
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@router.post("/get_text_sentiment")
async def get_text_sentiment(texts: str):
    try:
        result = txt_analyzer(texts)
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@router.get("/financial_summary")
async def financial_summary(texts: str):
    try:
        result = txt_summation(texts)
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@router.post("/get_tweets_sentiment")
def get_tweets_sentiment(search_requirement: dict):
    try:
        content = search_requirement['content']
        hashtag = search_requirement['hashtag']
        cashtag = search_requirement['cashtag']
        url = search_requirement['url']
        from_user = search_requirement['from_user']
        to_user = search_requirement['to_user']
        at_user = search_requirement['at_user']
        city = search_requirement['city']
        since = search_requirement['since']
        until = search_requirement['until']
        within_time = search_requirement['within_time']

        tweets_num = search_requirement['tweets_num']

        requirement = convert_twitter_search(content, hashtag, cashtag, url, from_user, to_user,
                                             at_user, city, since, until, within_time)
        result = tweets_analyzer(requirement, tweets_num)

        for k, v in result.items():
            result[k] = v.to_json(orient='records')
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@router.get("/get_reddits_sentiment")
async def get_reddits_sentiment(search_requirement: str, tweets_num: int = None):
    try:
        result = reddits_analyzer(search_requirement, tweets_num)
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)
