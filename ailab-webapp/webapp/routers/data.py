import traceback
from fastapi import APIRouter
from sqlalchemy import create_engine

from .. import config
from ..data.stock import get_top_gainers, get_top_losers
from ..webapp_models.generic_models import ResultResponse, CDSData
import numpy as np
import pandas as pd

router = APIRouter(
    prefix="/data",
    tags=["data"]
)


@router.get('/stock/get_top_gainers/{time_range}')
def get_top_gainers_api(time_range):
    try:
        df = get_top_gainers(time_range)[['Symbol', 'Name', 'Price (Intraday)', '% Change']]
        result = df.to_json(orient='records')
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@router.get('/stock/get_top_losers/{time_range}')
def get_top_losers_api(time_range):
    try:
        df = get_top_losers(time_range)[['Symbol', 'Name', 'Price (Intraday)', '% Change']]
        result = df.to_json(orient='records')
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@router.post("/data_warehouse/get_cds_data")
def get_cds_data(requestBody: CDSData):
    mysql_engine = create_engine(config.MYSQL_CONNECTION_URL)
    try:
        requestBody = requestBody.dict()
        region_query = f"""REGION in ({', '.join(f'"{w}"' for w in requestBody['REGION'])}) """ if requestBody[
                                                                                                       'REGION'] and len(
            requestBody['REGION']) > 0 else ''
        industry_query = f"""AND INDUSTRY in ({', '.join(f'"{w}"' for w in requestBody['INDUSTRY'])}) """ if \
            requestBody['INDUSTRY'] and len(requestBody['INDUSTRY']) > 0 else ''
        obligation_assetrank_query = f"""AND OBLIGATION_ASSETRANK in ({', '.join(f'"{w}"' for w in requestBody['OBLIGATION_ASSETRANK'])}) """ if \
            requestBody['OBLIGATION_ASSETRANK'] and len(requestBody['REGION']) > 0 else ''
        credit_events_query = f"""AND CREDIT_EVENTS in ({', '.join(f'"{w}"' for w in requestBody['CREDIT_EVENTS'])}) """ if \
            requestBody['CREDIT_EVENTS'] and len(requestBody['CREDIT_EVENTS']) > 0 else ''
        currency_query = f"""AND CURRENCY in ({', '.join(f'"{w}"' for w in requestBody['CURRENCY'])})""" if requestBody[
                                                                                                                'CURRENCY'] and len(
            requestBody['CURRENCY']) > 0 else ''
        conditions = region_query + industry_query + obligation_assetrank_query + credit_events_query + currency_query
        where_clause = 'WHERE ' + conditions if conditions != '' else ''
        limit = f'limit {requestBody["limit"]}' if requestBody["limit"] else ""
        query = "SELECT * from fermi.cds " + where_clause + limit
        df = pd.read_sql(query, mysql_engine)
        df.replace({np.nan: None}, inplace=True)
        result = df.to_json(orient="records")

    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@router.get("/data_warehouse/cds_get_unique_val/{param}")
def cds_get_unique_val(param: str):
    mysql_engine = create_engine(config.MYSQL_CONNECTION_URL)

    try:

        query = f"SELECT DISTINCT {param} from CDS.CDSData"
        df = pd.read_sql(query, mysql_engine)
        result = list(df[param])

    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)