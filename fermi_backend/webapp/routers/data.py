import re
from datetime import datetime
import traceback
from typing import List
import logging
from fastapi import APIRouter, Query
import pickle
from fermi_backend.webapp.data.stock import get_top_gainers, get_top_losers, get_hist_stock_price, get_single_hist_price
from .. import CONSTS
from ..data.wrds_api import AsyncWRDS
from ..exceptions import router_error_handler
from ..webapp_models.generic_models import ResultResponse, CDSData
from ..helpers import sql_session_scope, parse_sql_results
from fermi_backend.webapp import redis_cache

router = APIRouter(
    prefix="/data",
    tags=["data"]
)

logger = logging.getLogger(__name__)
wrds_api = AsyncWRDS()


async def set_up_metadata():
    logger.info("Loading metadata...")
    await wrds_api.init()
    if not redis_cache.exists("AVAIL_JOIN_TABLES"):
        avail_products = await wrds_api.get_avail_products()
        redis_cache.set("AVAIL_JOIN_TABLES", pickle.dumps(avail_products))
    if not redis_cache.exists("WRDS_META"):
        metadata = await wrds_api.get_products_dict(pickle.loads(redis_cache.get("AVAIL_JOIN_TABLES")))
        redis_cache.set("WRDS_META", pickle.dumps(metadata))
    if not redis_cache.exists("AVAIL_ASSET_IDENTIFIERS"):
        redis_cache.set("AVAIL_ASSET_IDENTIFIERS", pickle.dumps(["tic", "cusip", "gvkey"]))
    if not redis_cache.exists("AVAIL_JOINS"):
        redis_cache.set("AVAIL_JOINS", pickle.dumps(["INNER JOIN", "LEFT JOIN", "RIGHT JOIN"]))
    logger.info("Finished loading metadata")


async def clear_cache():
    logger.info("cleaning redis cache")
    redis_cache.flushdb()


@router.get('/stock/get_top_gainers/{time_range}')
def get_top_gainers_api(time_range):
    try:
        df = get_top_gainers(time_range)[['Symbol', 'Name', 'Price (Intraday)', '% Change']]
        result = df.to_json(orient='records')
    except Exception as e:
        return ResultResponse(status_code=CONSTS.HTTP_500_INTERNAL_SERVER_ERROR,
                              message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}")
    return ResultResponse(status_code=CONSTS.HTTP_200_OK, result=result)


@router.get('/stock/get_top_losers/{time_range}')
def get_top_losers_api(time_range):
    try:
        df = get_top_losers(time_range)[['Symbol', 'Name', 'Price (Intraday)', '% Change']]
        result = df.to_json(orient='records')
    except Exception as e:
        return ResultResponse(status_code=CONSTS.HTTP_500_INTERNAL_SERVER_ERROR,
                              message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status_code=CONSTS.HTTP_200_OK, result=result)


@router.get("/load_hist_stock_price/{start_date}/{end_date}")
async def load_hist_stock_price(start_date, end_date, q: List[str] = Query(None)):
    stock_price = get_hist_stock_price(tickers=q, start_date=start_date, end_date=end_date)
    return {i: stock_price[i].to_list() for i in stock_price.columns}


@router.get("/load_single_hist_stock_price/{ticker}/{start_date}/{end_date}")
def load_full_hist_stock_price(ticker, start_date, end_date):
    try:
        result = get_single_hist_price(ticker, start_date, end_date)
        result = result.to_json(orient='records')
    except Exception as e:
        return ResultResponse(status_code=CONSTS.HTTP_500_INTERNAL_SERVER_ERROR,
                              message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status_code=CONSTS.HTTP_200_OK, result=result)


@router.post("/data_warehouse/get_cds_data")
async def get_cds_data(requestBody: CDSData):
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
        query = "SELECT * from fermi_db.cds " + where_clause + limit
        async with sql_session_scope() as session:
            result = await session.execute(query)
            result = parse_sql_results(result)
        # df.replace({np.nan: None}, inplace=True)
        # result = df.to_json(orient="records")

    except Exception as e:
        return ResultResponse(status_code=CONSTS.HTTP_500_INTERNAL_SERVER_ERROR,
                              message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status_code=CONSTS.HTTP_200_OK, result=result)


@router.get("/data_warehouse/cds_get_unique_val/{param}")
async def cds_get_unique_val(param: str):
    async with sql_session_scope() as session:
        result = await session.execute(f"""SELECT DISTINCT {param} from fermi_db.cds""")
        result = parse_sql_results(result)
        result = [d[param] for d in result]

    return ResultResponse(status_code=CONSTS.HTTP_200_OK, result=result)


# TODO: change to post w/ user specified usrname and pwd, for their freedom of using their WRDS account
@router.get("/data_warehouse/fetch_available_tables")
@router_error_handler
async def fetch_available_tables():
    if redis_cache.exists("AVAIL_JOIN_TABLES"):
        wrds_meta = pickle.loads(redis_cache.get("WRDS_META"))
        tables = [f"{db_name}.{table_name}" for db_name in wrds_meta.keys() for table_name in wrds_meta[db_name].keys()]
    else:
        raise Exception("Available tables not found")

    return ResultResponse(result=[{'value': i, 'label': i} for i in tables],
                          status_code=CONSTS.HTTP_200_OK, message=f"Successfully queried available tables",
                          date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))


@router.get("/data_warehouse/fetch_available_identifiers")
@router_error_handler
async def fetch_available_identifiers():
    avail_asset_identifiers = [{'value': i, 'label': i} for i in
                               pickle.loads(redis_cache.get("AVAIL_ASSET_IDENTIFIERS"))]
    return ResultResponse(result=avail_asset_identifiers, status_code=CONSTS.HTTP_200_OK,
                          message=f"Successfully queried available tables",
                          date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))


@router_error_handler
@router.get("/data_warehouse/fetch_available_joins")
async def fetch_available_joins():
    avail_asset_identifiers = [{'value': i, 'label': i} for i in pickle.loads(redis_cache.get("AVAIL_JOINS"))]
    return ResultResponse(result=avail_asset_identifiers, status_code=CONSTS.HTTP_200_OK,
                          message=f"Successfully queried available tables",
                          date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))


@router.post("/data_warehouse/get_table_cols")
@router_error_handler
async def get_table_cols(request: dict):
    result_dict = {}
    table_names = request['table_columns'] # [db_name.table_name, ...]
    # async with mysql_session_scope() as session:
    #     for table in table_names:
    #         result = await session.execute(f"DESCRIBE {table}")
    #         result = parse_sql_results(result)
    #         result = [i['Field'] for i in result]
    #         result = [{'value': i, 'label': i} for i in result]
    #         result_dict[table] = result
    if redis_cache.exists("WRDS_META"):
        lookup_dict = pickle.loads(redis_cache.get("WRDS_META"))
    else:
        lookup_dict = None
    for table in table_names: # DB_Name.table_name
        split = table.split('.')
        db, table_name = split[0], split[1]
        if lookup_dict:
            cols = [f"{t[0]} -- {t[1]}" for t in lookup_dict[db][table_name]]  # wrds_meta: list of (col_name, dtype)
        else:
            cols = await wrds_api.get_table_cols(db, table_name)
            cols = [f"{t[0]} -- {t[1]}" for t in cols]
        result_dict[table] = [{'value': i, 'label': i} for i in cols]

    return ResultResponse(result=result_dict, status_code=CONSTS.HTTP_200_OK,
                          message=f"Successfully queried table columns",
                          date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))


@router.post("/data_warehouse/get_integrated_data")
@router_error_handler
async def get_integrated_data(request: dict):

    requestBody = request
    table_names = requestBody['table_names']
    identifier_type = requestBody['identifier_type']
    identifier_value = requestBody['identifier_value']
    join_types = requestBody['join_types']
    join_cols = requestBody['join_cols']
    start_date = requestBody['start_date']
    end_date = requestBody['end_date']
    table_columns = requestBody['table_columns']
    limit = requestBody['limit']

    if len(join_types) != len(table_names) - 1:
        raise Exception('The number of join type does not match the number of tables to join')

    # yyyymmdd_regex = "^20[1-2][0-9]-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])"
    # quarter_regex = "^20[1-2][0-9][qQ][1-4]$"
    # if re.match(yyyymmdd_regex, start_date) and re.match(yyyymmdd_regex, end_date):
    #     date_identifier = 'Date_General'
    # elif re.match(quarter_regex, start_date) and re.match(quarter_regex, end_date):
    #     date_identifier = 'Quarter'
    # else:
    #     raise Exception('Incorrect date format (Must be either YYYY-MM-DD or YYYYQ[1-4])')

    cols = ""
    for k, v in table_columns.items():
        table_cols = ",".join([f"{k}.{col}" for col in v]) + ','
        cols += table_cols
    cols = cols[:-1] # get rid of trailing ','
    # cols += table_names[0] + '.' + date_identifier
    # TODO: Assume the universal date column is called 'datadate'?
    date_identifier = 'datadate'
    cols += ',' + table_names[0] + '.' + date_identifier if date_identifier not in cols else ""

    from_tables = ""
    if len(table_names) == 1:
        from_tables = table_names[0]
    else:
        for idx in range(len(table_names) - 1):
            on = ""
            for join_idx in range(len(join_cols[table_names[idx]])):
                on += f"{table_names[idx]}.{join_cols[table_names[idx]][join_idx]} = " \
                      f"{table_names[idx + 1]}.{join_cols[table_names[idx + 1]][join_idx]}"
                if join_idx < len(join_cols[table_names[idx]]) - 1: on += " AND "
            from_tables += f"{table_names[idx]} {join_types[idx]} {table_names[idx + 1]} ON {on}"
    limit = " LIMIT " + limit if limit else ""

    result = await wrds_api.raw_sql(f"""SELECT {cols} FROM {from_tables} WHERE {table_names[0]}.{date_identifier} >= '{start_date}'
    AND {table_names[0]}.{date_identifier} <= '{end_date}' AND {table_names[0]}.{identifier_type} = '{identifier_value}'
    ORDER BY {table_names[0]}.{date_identifier} {limit}""")
    result = result.to_dict(orient='list')
    return ResultResponse(result=result, status_code=CONSTS.HTTP_200_OK, message=f"Successfully queried data",
                          date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))

