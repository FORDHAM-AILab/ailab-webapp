import datetime
import logging
from typing import List

from botocore.exceptions import ClientError
import re

from starlette.middleware.cors import CORSMiddleware
from python_aws import s3
from python_aws.utils import save_upload_file
from fastapi import FastAPI, File, UploadFile, Query
import uvicorn
import traceback
from webapp.server import CONSTS
import boto3
from webapp.data.stock import *
from webapp.data.options import *
from models.Binomial import BinomialTree
from models.Geske import Geske
from models.Options import options
from models.Portfolio.portfolio import Portfolio
from webapp.server.generic import ResultResponse, ReceiveTag, Data, CDSData
import pandas as pd
import numpy as np
from datetime import datetime
from webapp.config import ANALYTICS_DECIMALS
from webapp.server.helpers import round_result
from sqlalchemy import create_engine


app = FastAPI()

# TODO: cache the current object

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


mysql_connection_url = "mysql+pymysql://root:Tiger980330!@localhost/cds"
mysql_connection = create_engine(mysql_connection_url)


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
        data, weights, level, decay, n = pd.DataFrame(requestbody['data']), requestbody['weights'], requestbody['level'], requestbody['alpha'], requestbody['n']
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
        data, weights, expected_return = pd.DataFrame(requestbody['data']), requestbody['weights'], requestbody['expected_return']
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
def options_pricing_api(request_body:dict):
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
    try:
        requestBody = requestBody.dict()
        region_query = f"""REGION in ({', '.join(f'"{w}"' for w in requestBody['REGION'])}) """ if requestBody['REGION'] and len(requestBody['REGION']) > 0 else ''
        industry_query = f"""AND INDUSTRY in ({', '.join(f'"{w}"' for w in requestBody['INDUSTRY'])}) """ if requestBody['INDUSTRY'] and len(requestBody['INDUSTRY']) > 0 else ''
        obligation_assetrank_query = f"""AND OBLIGATION_ASSETRANK in ({', '.join(f'"{w}"' for w in requestBody['OBLIGATION_ASSETRANK'])}) """ if requestBody['OBLIGATION_ASSETRANK'] and len(requestBody['REGION']) > 0 else ''
        credit_events_query = f"""AND CREDIT_EVENTS in ({', '.join(f'"{w}"' for w in requestBody['CREDIT_EVENTS'])}) """ if requestBody['CREDIT_EVENTS'] and len(requestBody['CREDIT_EVENTS']) > 0 else ''
        currency_query = f"""AND CURRENCY in ({', '.join(f'"{w}"' for w in requestBody['CURRENCY'])})""" if requestBody['CURRENCY'] and len(requestBody['CURRENCY']) > 0 else ''
        conditions = region_query + industry_query + obligation_assetrank_query + credit_events_query + currency_query
        where_clause = 'WHERE ' + conditions if conditions != '' else ''
        limit = f'limit {requestBody["limit"]}' if requestBody["limit"] else ""
        query = "SELECT * from cds.CDSData " + where_clause + limit
        df = pd.read_sql(query, mysql_connection)
        df.replace({np.nan: None}, inplace=True)
        result = df.to_json(orient="records")

    except Exception as e:
        return ResultResponse(status=-1, message=f"An exception occurred {str(e)}:\n{traceback.format_exc()}", )
    return ResultResponse(status=0, result=result)


@app.get("/data/data_warehouse/cds_get_unique_val/{param}", tags=['data'])
def cds_get_unique_val(param: str):
    try:

        query = f"SELECT DISTINCT {param} from cds.CDSData"
        df = pd.read_sql(query, mysql_connection)
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
                              date_done=str(datetime.datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed', message=f"get file structure {bucket_name}/{key}", result=result,
                              date_done=str(datetime.datetime.utcnow().isoformat()))


@app.post("/s3/upload_s3_object/bucket/object", tags=['s3'])
async def upload_s3_object(bucket: str, key: str, file: UploadFile = File(...)
                           # ,current_user: User = Depends(get_current_user)
                           ):
    file_name = save_upload_file(file, CONSTS.TEMP_PATH.joinpath(file.filename))
    try:
        s3.upload_file_to_bucket(bucket, file_name, key)
    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed', message=f"Upload file to {bucket}/{key}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))


async def delete(bucket_name: str, isDirectory: bool, key: str):
    try:

        s3_object = boto3.resource('s3')
        objects_to_delete = s3_object.meta.client.list_objects(Bucket=bucket_name, Prefix=key)

        delete_keys = {'Objects': []}
        delete_keys['Objects'] = [{'Key': k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]

        s3_object.meta.client.delete_objects(Bucket=bucket_name, Delete=delete_keys)

    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed',
                              message=f"Delete {'directory' if isDirectory else 'file'} {bucket_name}/{key}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))


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
                              date_done=str(datetime.datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed',
                              message=f"Moved file {bucket_name}/{key} --> to --> {bucket_name}/{path}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))


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
                              date_done=str(datetime.datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed',
                              message=f"Rename {'directory' if isDirectory else 'file'} {bucket_name}/{key} --> to --> {bucket_name}/{path} ",
                              date_done=str(datetime.datetime.utcnow().isoformat()))


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
                              date_done=str(datetime.datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed', message=f"Create directory to {bucket_name}/{key}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))


@app.post("/s3/create_s3_file/bucket/key", tags=['s3'])
async def create_s3_file(bucket_name: str, key: str):
    try:
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Body='', Key=key)
    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed', message=f"Create file to {bucket_name}/{key}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))


if __name__ == '__main__':
    uvicorn.run('app:app', port=8888, host='127.0.0.1', log_level="info", reload=True)
