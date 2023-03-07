import numpy as np
from sqlalchemy import create_engine

from fermi_backend.webapp import helpers
import pandas as pd
import asyncio
from fermi_backend.webapp.exceptions import exception_handling
import logging

logger = logging.getLogger(__name__)

async def insert_into_sql(df: pd.DataFrame, table_name: str, extra_space: int = 5):

    # TODO: pandas df.to_sql shall fail on duplications

    async with helpers.mysql_session_scope() as session:
        table = await session.execute(f"""SHOW TABLES LIKE '{table_name}' """)
        table = helpers.sql_to_dict(table)
        # if table already exists, just insert
        sync_engine = create_engine('mysql+pymysql://root:Tiger980330!@localhost/fermi_db')
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



def format_df(path, index_col: int = None):
    if path.endswith('.csv'):
        df = pd.read_csv(path, index_col=index_col, thousands=',')
    elif path.endswith(('xls', 'xlsx')):
        df = pd.read_excel(path, index_col=index_col)
    else:
        raise Exception('Unsupported file extension.')
    # NOTE: This is very ad-hoc
    if 'YYYYMMDD' in df: # from WRDS-CRSP
        df['Date_General'] = df['YYYYMMDD'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d')).dt.date
    elif 'datadate' in df: # from WRDS-COMPUSTAT-CAPIQ
        df['Date_General'] = df['datadate'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d')).dt.date

    if 'PERMNO' in df:
        df['PERMNO'] = df['PERMNO'].astype('str')
    if 'CUSIP' in df:
        df['CUSIP'] = df['CUSIP'].astype('str')

    return df

if __name__ == '__main__':
    df = format_df('/Users/xuanmingcui/Downloads/DJ30-5yr-stocks.csv')
    print(df.head())
    asyncio.run(insert_into_sql(df, 'cds_test'))
