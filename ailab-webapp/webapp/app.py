import json
from datetime import datetime, timedelta
import pytz
import logging
import uuid
from collections import defaultdict

from botocore.exceptions import ClientError
import re

from sqlalchemy import create_engine

from webapp.auth.db_authclients import get_db_client

from starlette.middleware.cors import CORSMiddleware
from python_aws import s3
from python_aws.utils import save_upload_file
from fastapi import FastAPI, File, UploadFile, Query, Depends
import uvicorn
import traceback
from webapp import CONSTS, helpers
import boto3
from webapp.data.stock import *
from webapp.data.get_options import *
from models.Options import options
from models.Portfolio.portfolio import Portfolio
from webapp.webapp_models.db_models import InternalUser
from webapp.webapp_models.generic_models import ResultResponse, Data, CDSData
import pandas as pd
import numpy as np
from webapp.CONSTS import ANALYTICS_DECIMALS
from webapp.helpers import round_result
from starlette.config import Config
from starlette.requests import Request
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import RedirectResponse, JSONResponse
from authlib.integrations.starlette_client import OAuth
from webapp.webapp_models.auth_models import ExternalAuthToken, InternalAccessTokenData
from fastapi.encoders import jsonable_encoder
from webapp.auth import providers as auth_providers, schemes as auth_schemes, util as auth_util
from webapp.exceptions import AuthorizationException, exception_handling
from webapp import config

from models.VaR import HistoricalVaR, PCAVaR

app = FastAPI()

# TODO: cache the current object

HTTP_ORIGIN = ['http://127.0.0.1:8888',
               'http://localhost:3000']

app.add_middleware(
    CORSMiddleware,
    allow_origins=HTTP_ORIGIN,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.add_middleware(SessionMiddleware, secret_key='!secret')

csrf_token_redirect_cookie_scheme = auth_schemes.CSRFTokenRedirectCookieBearer()
auth_token_scheme = auth_schemes.AuthTokenBearer()
access_token_cookie_scheme = auth_schemes.AccessTokenCookieBearer()

# Initialize db client
mysqldb_client = get_db_client(config.DATABASE_TYPE)


@app.middleware("http")
async def setup_request(request: Request, call_next) -> JSONResponse:
    """
    A middleware for setting up a request. It creates a new request_id
    and adds some basic metrics.
    Args:
        request: The incoming request
        call_next (obj): The wrapper as per FastAPI docs
    Returns:
        response: The JSON response
    """
    response = await call_next(request)

    return response


@app.get("/login-redirect")
async def login_redirect(auth_provider: str):
    """
    Redirects the user to the external authentication pop-up
    Args:
        auth_provider: The authentication provider (i.e google-iodc)
    Returns:
        Redirect response to the external provider's auth endpoint
        """
    async with exception_handling():
        provider = await auth_providers.get_auth_provider(auth_provider)

        request_uri, state_csrf_token = await provider.get_request_uri()

        response = RedirectResponse(url=request_uri)

        # Make this a secure cookie for production use
        response.set_cookie(key="state", value=f"Bearer {state_csrf_token}", httponly=True)

        return response


@app.get("/google-login-callback/")
async def google_login_callback(
        request: Request,
        _=Depends(csrf_token_redirect_cookie_scheme)  # TODO: check this depends
):
    """
    Callback triggered when the user logs in to Google's pop-up.
    Receives an authentication_token from Google which then
    exchanges for an access_token. The latter is used to
    gain user information from Google's userinfo_endpoint.
    Args:
        request: The incoming request as redirected by Google
    """
    async with exception_handling():
        code = request.query_params.get("code")

        if not code:
            raise AuthorizationException("Missing external authentication token")

        provider = await auth_providers.get_auth_provider(config.GOOGLE)

        # Authenticate token and get user's info from external provider
        external_user = await provider.get_user(
            auth_token=ExternalAuthToken(code=code)
        )

        # Get or create the internal user
        internal_user = await mysqldb_client.get_user_by_external_sub_id(external_user)

        if internal_user is None:
            internal_user = await mysqldb_client.create_internal_user(external_user)

        internal_auth_token = await auth_util.create_internal_auth_token(internal_user)

        # Redirect the user to the home page
        home_url = 'http://localhost:3000/admin/home'

        redirect_url = f"{home_url}?authToken={internal_auth_token}"
        response = RedirectResponse(url=redirect_url)

        # Delete state cookie. No longer required
        response.delete_cookie(key="state")

        return response


@app.get("/login/")
async def login(response: JSONResponse, internal_user: InternalUser = Depends(auth_token_scheme)) -> JSONResponse:
    """
    Login endpoint for authenticating a user after he has received
    an authentication token. If the token is valid it generates
    an access token and inserts it in a HTTPOnly cookie.
    :param internal_user:
    :param response:

    Returns:
        response: A JSON response with the status of the user's session


    """
    async with exception_handling():
        access_token = await auth_util.create_internal_access_token(
            InternalAccessTokenData(
                sub=internal_user.internal_sub_id,
            )
        )

        response = JSONResponse(
            content=jsonable_encoder({
                "userLoggedIn": True,
                "userName": internal_user.username,
            }),
        )

        # TODO: Make this a secure cookie for production use
        response.set_cookie(key="access_token", value=f"Bearer {access_token}", httponly=True)

        return response


@app.get("/logout/")
async def logout(
        response: JSONResponse,
        internal_user: str = Depends(access_token_cookie_scheme)
) -> JSONResponse:
    """
    Logout endpoint for deleting the HTTPOnly cookie on the user's browser.
    Args:
        internal_auth_token: Internal authentication token
    Returns:
        response: A JSON response with the status of the user's session
    """
    async with exception_handling():
        response = JSONResponse(
            content=jsonable_encoder({
                "userLoggedIn": False,
            }),
        )

        response.delete_cookie(key="access_token")

        return response


@app.get("/user-session-status/")
async def user_session_status(
        internal_user: InternalUser = Depends(access_token_cookie_scheme)
) -> JSONResponse:
    """
    User status endpoint for checking whether the user currently holds
    an HTTPOnly cookie with a valid access token.
    Args:
        internal_user:
    Returns:
        response: A JSON response with the status of the user's session
    """
    async with exception_handling():
        logged_id = True if internal_user else False
        username = internal_user.username if internal_user else None
        response = JSONResponse(
            content=jsonable_encoder({
                "userLoggedIn": logged_id,
                "userName": username,
                "userInfo": internal_user.dict() if internal_user else None
            }),
        )

        return response


@app.get('/home/get_top_gainers/{time_range}', tags=['home'])
def get_top_gainers_api(time_range):
    try:
        df = get_top_gainers(time_range)[['Symbol', 'Name', 'Price (Intraday)', '% Change']]
        result = df.to_json(orient='records')
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@app.get('/home/get_top_losers/{time_range}', tags=['home'])
def get_top_losers_api(time_range):
    try:
        df = get_top_losers(time_range)[['Symbol', 'Name', 'Price (Intraday)', '% Change']]
        result = df.to_json(orient='records')
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@app.post("/portfolio_analysis/get_basic_info", tags=['portfolio_analysis'])
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
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@round_result(ANALYTICS_DECIMALS)
def get_all_var(data, weights, level, decay, n):
    p = Portfolio(data, weights)
    result = {'Historical VaR': p.hvar(level),
              'Parametric VaR': p.pvar(level / 100, alpha=decay),
              'Monte Carlo VaR': p.monte_carlo_var(level, n)}
    return result


@app.post("/portfolio_analysis/valueatrisk", tags=['portfolio_analysis'])
def valueatrisk(requestbody: dict) -> ResultResponse:
    try:
        data, weights, level, decay, n = pd.DataFrame(requestbody['data']), requestbody['weights'], requestbody[
            'level'], requestbody['alpha'], requestbody['n']
        result = get_all_var(data, weights, level, decay, n)
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@round_result(ANALYTICS_DECIMALS)
def weights_optimization(data, weights, expected_return):
    p = Portfolio(data, weights=weights)
    return list(p.optimization(given_return=expected_return))


@app.post("/portfolio_analysis/weights_optimization", tags=['portfolio_analysis'])
def weights_optimization_api(requestbody: dict) -> ResultResponse:
    try:
        data, weights, expected_return = pd.DataFrame(requestbody['data']), requestbody['weights'], requestbody[
            'expected_return']
        result = weights_optimization(data, weights, expected_return)
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@app.post("/portfolio_analysis/sharpe_ratio", tags=['portfolio_analysis'])
def sharpe_ratio(requestbody: dict) -> ResultResponse:
    try:
        df = pd.DataFrame(requestbody['data'])
        p = Portfolio(df, weights=requestbody['weights'])
        result = p.sharpe_r(requestbody['rf'])
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@app.get("/data/load_hist_stock_price/{start_date}/{end_date}", tags=['data'])
def load_hist_stock_price(start_date, end_date, q: List[str] = Query(None)):
    stock_price = get_hist_stock_price(tickers=q, start_date=start_date, end_date=end_date)
    return {i: stock_price[i].to_list() for i in stock_price.columns}


@app.get("/data/load_single_hist_stock_price/{ticker}/{start_date}/{end_date}", tags=['data'])
def load_full_hist_stock_price(ticker, start_date, end_date):
    try:
        result = get_single_hist_price(ticker, start_date, end_date)
        result = result.to_json(orient='records')
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@app.get("/stock/get_analysis_info/{ticker}", tags=['stock'])
def get_analysis_info_api(ticker):
    try:
        result = get_analysis_info(ticker)
        for k, v in result.items():
            result[k] = v.to_json(orient='records')
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@app.get("/options/get_options_expiration_date/{ticker}")
def get_options_expiration_date_api(ticker):
    try:
        result = get_options_expiration_date(ticker)
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@app.post("/options/get_options_data_api")
def get_options_data_api(requestBody: dict):
    try:
        result = get_options_data(requestBody['ticker'], datetime.strptime(requestBody['date'], '%B %d, %Y'),
                                  requestBody['options_type'])
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@app.post("/options/options_pricing", tags=['options'])
def options_pricing_api(request_body: dict):
    try:
        s, k, rf, div, vol, T, options_type, N, method = request_body['s'], request_body['k'], request_body['rf'], \
                                                         request_body['div'], request_body['vol'], request_body['T'], \
                                                         request_body['options_type'], request_body['N'], request_body[
                                                             'method']
        if method == 'BS':
            pc_flag = 1 if options_type == 'call' else -1
            result = options.bs(s, k, rf, div, vol, T, pc_flag)
        elif method == 'Binomial Tree':
            result = options.binomial_tree(s, k, T, rf, vol, N, options_type == 'call')
        else:
            result = 0
    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@app.post("/data/data_warehouse/get_cds_data", tags=['data'])
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
        query = "SELECT * from CDS.CDSData " + where_clause + limit
        df = pd.read_sql(query, mysql_engine)
        df.replace({np.nan: None}, inplace=True)
        result = df.to_json(orient="records")

    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@app.get("/data/data_warehouse/cds_get_unique_val/{param}", tags=['data'])
def cds_get_unique_val(param: str):
    mysql_engine = create_engine(config.MYSQL_CONNECTION_URL)

    try:

        query = f"SELECT DISTINCT {param} from CDS.CDSData"
        df = pd.read_sql(query, mysql_engine)
        result = list(df[param])

    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@app.post("/s3/get_file_structure/bucket/key", tags=['s3'])
async def get_file_structure(bucket_name: str, key: str):
    try:
        result = []
        s3_obj = boto3.resource('s3')
        my_bucket = s3_obj.Bucket(bucket_name)
        for object_summary in my_bucket.objects.filter(Prefix=key):
            file_dict = {}
            if object_summary.key != key and object_summary.key != key + '/':
                file_dict['name'] = [i for i in object_summary.key.split('/') if i != ''][-1]
                file_dict['path'] = object_summary.key
                if object_summary.key[-1] == '/':
                    file_dict['IsDirectory'] = True
                    if len(list(my_bucket.objects.filter(Prefix=object_summary.key))) > 1:
                        file_dict['HasChild'] = True

                    else:
                        file_dict['HasChild'] = False
                else:
                    file_dict['IsDirectory'] = False
                    file_dict['HasChild'] = False
                #     file_dict['HasChild'] = False
                # location = boto3.client('s3').get_bucket_location(Bucket=bucket_name)['LocationConstraint']
                # url = "https://s3-%s.amazonaws.com/%s/%s" % (location, bucket_name, object_summary.key)
                url = s3.generate_presigned_url(bucket=bucket_name, key=object_summary.key,
                                                expiration=CONSTS.PRESIGNED_URL_LIFE)
                file_dict['FileUrl'] = url
            result.append(file_dict)

    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed', message=f"get file structure {bucket_name}/{key}", result=result,
                              date_done=str(datetime.utcnow().isoformat()))


@app.post("/s3/upload_s3_object/bucket/object", tags=['s3'])
async def upload_s3_object(bucket: str, key: str, file: UploadFile = File(...)
                           # ,current_user: User = Depends(get_current_user)
                           ):
    file_name = save_upload_file(file, CONSTS.TEMP_PATH.joinpath(file.filename))
    try:
        s3.upload_file_to_bucket(bucket, file_name, key)
    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed', message=f"Upload file to {bucket}/{key}",
                              date_done=str(datetime.utcnow().isoformat()))


async def delete(bucket_name: str, isDirectory: bool, key: str):
    try:

        s3_object = boto3.resource('s3')
        objects_to_delete = s3_object.meta.client.list_objects(Bucket=bucket_name, Prefix=key)

        delete_keys = {'Objects': []}
        delete_keys['Objects'] = [{'Key': k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]

        s3_object.meta.client.delete_objects(Bucket=bucket_name, Delete=delete_keys)

    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed',
                              message=f"Delete {'directory' if isDirectory else 'file'} {bucket_name}/{key}",
                              date_done=str(datetime.utcnow().isoformat()))


@app.post("/s3/delete_file/bucket/key", tags=['s3'])
async def delete_file(bucket_name: str, isDirectory: bool, key: str):
    return await delete(bucket_name, isDirectory, key)


@app.post("/s3/move_file/bucket/key/", tags=['s3'])
async def move_file(bucket_name: str, key: str, isDirectory: bool, path: str):
    try:
        s3_object = boto3.resource('s3')

        if isDirectory:
            key = key + '/' if key[-1] != '/' else key
            my_bucket = s3_object.Bucket(bucket_name)
            for object_summary in my_bucket.objects.filter(Prefix=key):
                new_key = object_summary.key.replace(key, path)[:-1] if object_summary.key.replace(key, path)[
                                                                        -2:] == '//' else object_summary.key.replace(
                    key, path)

                s3_object.Object(bucket_name, new_key).copy_from(CopySource=f'{bucket_name}/{object_summary.key}')
            await delete(bucket_name, True, key)
        ##
        else:
            s3_object.Object(bucket_name, path).copy_from(CopySource=f'{bucket_name}/{key}')
            await delete(bucket_name, isDirectory, key)

    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed',
                              message=f"Moved file {bucket_name}/{key} --> to --> {bucket_name}/{path}",
                              date_done=str(datetime.utcnow().isoformat()))


@app.post("/s3/rename_file/bucket/key/newname", tags=['s3'])
async def rename_file(bucket_name: str, key: str, isDirectory: bool, newname: str, path: str):
    try:
        s3_object = boto3.resource('s3')

        if isDirectory:
            key = key + '/' if key[-1] != '/' else key
            file_name_index = len([i for i in re.split('(/)', key) if i != '']) - 2
            my_bucket = s3_object.Bucket(bucket_name)
            for object_summary in my_bucket.objects.filter(Prefix=key):
                new_key_splitted = [i for i in re.split('(/)', object_summary.key) if i != '']
                new_key_splitted[file_name_index] = newname
                new_key = ''.join(new_key_splitted)
                s3_object.Object(bucket_name, new_key).copy_from(CopySource=f'{bucket_name}/{object_summary.key}')
                s3_object.Object(bucket_name, object_summary.key).delete()

        else:
            s3_object.Object(bucket_name, path).copy_from(CopySource=f'{bucket_name}/{key}')
            s3_object.Object(bucket_name, key).delete()

    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed',
                              message=f"Rename {'directory' if isDirectory else 'file'} {bucket_name}/{key} --> to --> {bucket_name}/{path} ",
                              date_done=str(datetime.utcnow().isoformat()))


@app.post("/s3/create_s3_bucket/bucket/key", tags=['s3'])
async def create_s3_bucket(bucket_name: str):
    try:

        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=bucket_name)

    except ClientError as e:
        logging.error(e)
        return False
    return True


@app.post("/s3/create_s3_dir/bucket/key", tags=['s3'])
async def create_s3_dir(bucket_name: str, key: str):
    try:
        s3 = boto3.client('s3')
        key = key + '/' if key[-1] != '/' else key
        s3.put_object(Bucket=bucket_name, Body='', Key=key)
    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed', message=f"Create directory to {bucket_name}/{key}",
                              date_done=str(datetime.utcnow().isoformat()))


@app.post("/s3/create_s3_file/bucket/key", tags=['s3'])
async def create_s3_file(bucket_name: str, key: str):
    try:
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Body='', Key=key)
    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed', message=f"Create file to {bucket_name}/{key}",
                              date_done=str(datetime.utcnow().isoformat()))


@app.post("/game/rm_game/create_rm_game_user", tags=['game'])
async def create_rm_game_user(internal_user: InternalUser):
    # = Depends(access_token_cookie_scheme)):
    eastern = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern).isoformat()
    with helpers.mysql_session_scope() as session:
        session.execute(
            f"""INSERT INTO game_rm_account (user_id, net_account_value,market_value,cash_balance, pl,pl_percent,updated_at, created_at, capital_gain) 
                VALUES ('{internal_user.internal_sub_id}',{CONSTS.GAME_RM_NOTIONAL},0,{CONSTS.GAME_RM_NOTIONAL},0,0,'{current_time}',
                        '{current_time}', 0)""")

    return ResultResponse(status=0, message=f"Create user: {internal_user.username} for RM game",
                          date_done=str(datetime.now(eastern).isoformat()))


@app.post("/game/rm_game/get_account_info", tags=['game'])
def get_account_info(internal_user: InternalUser):
    # = Depends(access_token_cookie_scheme)):
    eastern = pytz.timezone('US/Eastern')
    with helpers.mysql_session_scope() as session:
        result = session.execute(
            f"""SELECT * FROM game_rm_account WHERE user_id = '{internal_user.internal_sub_id}' """)
        result = helpers.sql_to_dict(result)

    if len(result) == 0:
        raise Exception(f'User: {internal_user.username} not registered for the game')

    return ResultResponse(status=0, result=result[0],
                          message=f"Found user: {internal_user.username}'s current account info",
                          date_done=str(datetime.now(eastern).isoformat()))


@app.post("/game/rm_game/update_portfolio", tags=['game'])
async def update_portfolio(request: dict):
    """
        :param request: Request object that contains transaction infos, e.g.: {'transaction':{"AAPL":10, "TSLA":4}}
        :param internal_user:
        :return:
    """
    eastern = pytz.timezone('US/Eastern')

    # TODO: remove below line when connected with frontend. This is only for testing:
    internal_user = InternalUser(username='foo', internal_sub_id='111', external_sub_id='222',
                                 created_at=datetime.now(eastern).isoformat(), email='foo@gmail.com')
    with helpers.mysql_session_scope() as session:
        result_current = session.execute(f"""SELECT * FROM game_rm_account WHERE 
                                             user_id = '{internal_user.internal_sub_id}' """)
        if result_current.rowcount == 0:
            raise Exception(f'User: {internal_user.username} not registered for the game')
        result_current = helpers.sql_to_dict(result_current)

    new_transaction = request  # TODO: uncomment this for prod: await request.json()
    new_transactions = new_transaction['transactions']  # {'AAPL':5, 'TSLA':10}
    net_account_value = float(result_current[0]['net_account_value'])
    market_value = float(result_current[0]['market_value'])
    cash_balance = float(result_current[0]['cash_balance'])
    # leverage allowed: cash can lend the same value as itself, stock can lend 0.8 times of its value
    buying_power = market_value * 0.8 + (cash_balance*2 if cash_balance>0 else cash_balance)
    current_time = datetime.now(eastern).isoformat()

    # if the user is new, i.e. current_shares dict is None, create empty defaultdict
    if result_current[0]['current_shares'] is None:
        new_shares = defaultdict(lambda: 0)
    # otherwise, modify from current's
    else:
        new_shares = defaultdict(lambda: 0, json.loads(result_current[0]['current_shares']))
    for ticker, n_shares in new_transactions.items():
        new_shares[ticker] += n_shares
    new_shares = dict(new_shares)
    # get today's price: TODO: 是否取分钟级数据 instead of 日级数据？ 需要修改yfinance包里的stock_info文件
    new_prices = get_hist_stock_price(list(new_shares.keys()), current_time, current_time).to_dict(orient='records')[0]
    # deduct from the current balance
    for ticker, shares in new_transactions.items():
        # insufficient buying power
        if buying_power < (shares * new_prices[ticker]): # Leverage allowed
            raise Exception(f"Insufficient buying power for transaction: {'Buy' if shares > 0 else 'Sell'} "
                            f"{ticker} {abs(shares)} shares")
        else:
            cash_balance -= shares * new_prices[ticker]

        # request: Request, internal_user: InternalUser = Depends(access_token_cookie_scheme)):


    # get the historical price table for one year for VaR
    annual_price = get_hist_stock_price(list(new_shares.keys()),
                                        datetime.now(eastern) - timedelta(days=365), datetime.now(eastern))
    current_weights = [new_shares[ticker] for ticker in annual_price.columns[:-1]]
    current_weights = [shares / sum(current_weights) for shares in current_weights]
    current_portfolio = Portfolio(df=annual_price, weights=current_weights)
    hist_var = current_portfolio.hvar()
    p_var = current_portfolio.pvar()
    monte_carlo_var = current_portfolio.monte_carlo_var()

    # get account market value
    market_value = 0
    for ticker, shares in new_shares.items():
        market_value += new_prices[ticker] * shares

    # update buying_power
    buying_power = market_value * 0.8 + (cash_balance * 2 if cash_balance > 0 else cash_balance)

    # update account, transaction and portfoliio info
    with helpers.mysql_session_scope() as session:
        # update account
        session.execute(f"""UPDATE game_rm_account SET updated_at = '{current_time}'
                                                   , net_account_value = {cash_balance + market_value},
                                                   , market_value = {market_value}
                                                   , cash_balance = {cash_balance}
                                                   , hist_var = {hist_var}
                                                   , p_var = {p_var}
                                                   , monte_carlo_var = {monte_carlo_var}
                                                   , pl = {cash_balance + market_value - CONSTS.GAME_RM_NOTIONAL}
                                                   , pl_percent = {round((cash_balance + market_value - CONSTS.GAME_RM_NOTIONAL)/100,2)}
                                                   , current_shares = '{json.dumps(new_shares)}'
                                                   WHERE user_id = '{internal_user.internal_sub_id}'""")

        # record new trades request
        for ticker, shares in new_transactions.items():
            session.execute(f"""INSERT INTO game_rm_transactions VALUES ('{uuid.uuid4()}', 
                                '{internal_user.internal_sub_id}','{current_time}', '{ticker}', {shares})""")

        # calculate pnl for each specific ticker
        for ticker, shares in new_shares.items():
            result_portfolio = session.execute(f"""SELECT * FROM game_rm_portfolio WHERE 
                                                               user_id = '{internal_user.internal_sub_id}' 
                                                               AND ticker = '{ticker}'""")
            if result_portfolio.rowcount == 0:
                session.execute(f"""INSERT INTO game_rm_portfolio VALUES ('{internal_user.internal_sub_id}', 
                                                '{ticker}',{round(shares * new_prices[ticker], 2)}, {shares}, 0, 0,
                                                {new_prices[ticker]},{new_prices[ticker]})""")
            else:
                result_portfolio = helpers.sql_to_dict(result_portfolio)

                average_price = float(result_portfolio[0]['average_price'])
                quantity = float(result_portfolio[0]['quantity'])

                new_price = new_prices[ticker]
                new_market_value = round(shares * new_price, 2)
                # update average_price
                average_price = round((average_price * quantity + new_price * new_transactions[ticker])/shares,2)

                open_pl = new_market_value - average_price * shares
                session.execute(f"""UPDATE game_rm_portfolio SET market_value = {new_market_value}
                                                                , quantity = {shares}
                                                                , open_pl = {open_pl}
                                                                , open_pl_percent = {round(open_pl/100,2)}
                                                                , last_price = {new_price}
                                                                , average_price = {average_price}
                                                                WHERE user_id = '{internal_user.internal_sub_id}'
                                                                AND ticker = '{ticker}'""")

    return ResultResponse(status=0, message=f"Transaction succeed for user: {internal_user.username}",
                          date_done=str(datetime.now(eastern).isoformat()))


@app.post("/game/rm_game/get_transaction_history", tags=['game'])
async def get_transaction_history(internal_user: InternalUser):
    eastern = pytz.timezone('US/Eastern')
    # = Depends(access_token_cookie_scheme)):
    """
    return list of
    :param internal_user: InternalUser class
    :return:
    """
    with helpers.mysql_session_scope() as session:
        result = session.execute(
            f"""SELECT * FROM game_rm_transactions WHERE user_id = '{internal_user.internal_sub_id}' 
                ORDER BY transaction_time DESC""")
        result = helpers.sql_to_dict(result)

    return ResultResponse(status=0, result=result, message=f"Transaction succeed for user: {internal_user.username}",
                          date_done=str(datetime.now(eastern).isoformat()))


@app.get("/game/rm_game/rank_players_rm/{by}", tags=['game'])
async def rank_players_rm(by: str = 'balance'):
    """
    rank game participants by the given field
    :param by:
    :return:
    """
    eastern = pytz.timezone('US/Eastern')
    with helpers.mysql_session_scope() as session:
        result = session.execute(
            f"""SELECT username, net_account_value FROM game_rm_account LEFT JOIN users 
                ON game_rm_account.user_id = users.internal_sub_id ORDER BY {by} DESC""")
        result = helpers.sql_to_dict(result)

    return ResultResponse(status=0, result={'result':result}, date_done=str(datetime.now(eastern).isoformat()))


@app.put("/game/rm_game/reset_game", tags=['game'])
async def reset_game(internal_user: InternalUser):
    eastern = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern).isoformat()
    with helpers.mysql_session_scope() as session:
        session.execute(f"""DELETE FROM game_rm_account WHERE user_id = '{internal_user.internal_sub_id}'""")
        session.execute(f"""DELETE FROM game_rm_transactions WHERE user_id = '{internal_user.internal_sub_id}'""")
        session.execute(f"""DELETE FROM game_rm_portfolio WHERE user_id = '{internal_user.internal_sub_id}'""")

    return ResultResponse(status=0, message=f"Reset user: {internal_user.username} for RM game",
                          date_done=str(datetime.now(eastern).isoformat()))


if __name__ == '__main__':
    uvicorn.run('app:app', port=8088, host='127.0.0.1', log_level="info", reload=True)
