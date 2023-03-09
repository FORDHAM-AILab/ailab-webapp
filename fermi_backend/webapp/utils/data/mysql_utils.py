import sys
import os
module_path = os.path.abspath(__file__)
sys.path.append(module_path[:module_path.index("webapp")])
sys.path.append(module_path[:module_path.index("fermi_backend")])

import copy
import traceback

import numpy as np
from sqlalchemy import create_engine
from fermi_backend.webapp.config import MYSQL_CONNECTION_URL_SYNC
from fermi_backend.webapp import helpers
import pandas as pd
import asyncio
from fermi_backend.webapp.exceptions import exception_handling
import logging
from fermi_backend.webapp.utils.data.google_drive_api import GoogleDriveAPI
import argparse

logging.basicConfig(
                    handlers=[
                        logging.FileHandler(f"{os.path.dirname(__file__)}/logging.log"),
                        logging.StreamHandler()
                    ],
                    level=logging.DEBUG,
                    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
                    datefmt='%H:%M:%S'
                    )
logger = logging.getLogger(__name__)


async def insert_into_sql(df: pd.DataFrame, table_name: str, extra_space: int = 5):

    # TODO: pandas df.to_sql shall fail on duplications

    async with helpers.mysql_session_scope() as session:
        table = await session.execute(f"""SHOW TABLES LIKE '{table_name}' """)
        table = helpers.sql_to_dict(table)
        # if table already exists, just insert
        sync_engine = create_engine(MYSQL_CONNECTION_URL_SYNC)
        if table:
            df.to_sql(table_name, sync_engine, if_exists='append', index=False)
        # else, create table from the given df
        else:

            create_query = ""
            len_per_col = np.vectorize(len)(df.values.astype(str)).max(axis=0)
            idx =0
            for name, length, dtype in zip(df.columns, len_per_col, list(df.dtypes)):
                if pd.api.types.is_string_dtype(dtype):
                    if name == 'Date_General': # str: YYYY-MM-DD
                        col_type = 'DATE'
                    else:
                        col_type = f"VARCHAR({length + extra_space})"
                elif pd.api.types.is_float_dtype(dtype):
                    # defined decimal places to be 8
                    col_type = f"DECIMAL({length + extra_space + 8}, 8)"
                elif pd.api.types.is_integer_dtype(dtype):
                    col_type = f"DECIMAL({length + extra_space})"
                else:
                    raise Exception(f"Unsupported datatype: {dtype}")

                create_query += name + " " + col_type

                if idx < len(len_per_col) -1:
                    create_query += ","

                idx += 1

            create_table_query = f"CREATE TABLE {table_name} ({create_query});"
            insert = await session.execute(create_table_query)
            df.to_sql(table_name, sync_engine, if_exists='append', index=False)



def format_df(path_or_df, index_col: int = None):
    if type(path_or_df) == str:
        if path_or_df.endswith('.csv'):
            df = pd.read_csv(path_or_df, index_col=index_col, thousands=',')
        elif path_or_df.endswith(('xls', 'xlsx')):
            df = pd.read_excel(path_or_df, index_col=index_col)
        else:
            raise Exception('Unsupported file extension.')
    else:
        df = copy.deepcopy(path_or_df)
    # NOTE: This is very ad-hoc
    if 'YYYYMMDD' in df: # from WRDS-CRSP
        df['Date_General'] = df['YYYYMMDD'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d')).dt.date
        df['Quarter'] = pd.PeriodIndex(df['Date_General'], freq='Q').astype(str)
    elif 'datadate' in df: # from WRDS-COMPUSTAT-CAPIQ
        df['Date_General'] = df['datadate'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d')).dt.date
        df['Quarter'] = pd.PeriodIndex(df['Date_General'], freq='Q').astype(str)

    if 'PERMNO' in df:
        df['PERMNO'] = df['PERMNO'].astype('str')
    if 'CUSIP' in df:
        df['CUSIP'] = df['CUSIP'].astype('str')

    return df


def download_data_to_sql(filename, table_name, source='Google', **kwargs):
    try:
        if source == 'Google':
            google_connector = GoogleDriveAPI(cred_dir_path=kwargs.get('cred_dir_path', os.path.dirname(os.path.abspath(__file__))))
            logger.info("==> Downloading data from drive")
            df = google_connector.get_csv_file(filename, path_loc=None)
            logger.info("==> Formatting data for sql insertion")
            df = format_df(df)
            logger.info("==> Inserting data into sql")
            asyncio.run(insert_into_sql(df, table_name=table_name))
            logger.info("==> Successfully inserted data.")
    except Exception as e:
        logger.error(repr(e), f'Traceback: {traceback.format_exc()}')



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--filename", type=str)
    parser.add_argument("--table_name", type=str)
    parser.add_argument("--source", type=str)

    args = parser.parse_args()
    download_data_to_sql(args.filename, args.table_name, args.source)
