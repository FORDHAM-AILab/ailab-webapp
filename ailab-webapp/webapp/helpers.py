from typing import List, Optional, Union

import pandas as pd
import numpy as np
from functools import wraps
import copy
from contextlib import contextmanager

from sqlalchemy.orm import scoped_session

from webapp import mysql_session_factory


def round_digit(x, decimal: int = 0):
    return x if (isinstance(x, (str, int)) or (x is None)) \
        else None if (np.isnan(x) or np.isinf(x)) \
        else int(round(x) + 0) if isinstance(x, float) and decimal == 0 \
        else round(x, decimal) if isinstance(x, float) and decimal != 0 \
        else None


def round_nested_dict_list(x, decimal: int):
    """
    recursively iter through dict and round
    :param result:
    :param x: dict
    :param decimal:
    :return:
    """

    def round_inner(x, result: Union[dict, list]):
        if isinstance(x, dict):
            for k, v in x.items():
                if isinstance(v, (dict, list)):
                    round_inner(v, result[k])
                else:
                    result[k] = round_digit(v, decimal)
        elif isinstance(x, list):
            for idx, item in enumerate(x):
                if isinstance(item, (dict, list)):
                    round_inner(item, result[idx])
                else:
                    result[idx] = round_digit(item, decimal)

    result = copy.deepcopy(x)
    round_inner(x, result)
    return result


def round_result(decimal: int = 4):
    """
    Wrapper function to replace nan/inf with None and round to {DETAIL_DECIMAL_PLACE} decimal points
    Accept float,dict,list and st
    """

    def wrapper_outer(func):
        @wraps(func)
        def round_wrapper(*args, **kwargs):

            result = func(*args, **kwargs)
            if isinstance(result, float):
                return round_digit(result, decimal)
            elif isinstance(result, (dict, list)):
                return round_nested_dict_list(result, decimal)
            elif isinstance(result, set):
                return {round_digit(x, decimal) for x in result}
            else:
                return result

        return round_wrapper

    return wrapper_outer


def round_pct(l: List[float], decimal: int) -> List[float]:
    """
    Round the portion to DETAIL_DECIMAL_PLACE while still sum up to 100
    """
    round_list = [0]
    round_list.extend(l)
    round_df = pd.DataFrame({'origin': round_list})
    round_df['cumsum'] = round_df['origin'].cumsum(axis=0).round(decimal).diff()
    return [round(i, decimal) for i in list(round_df['cumsum'])[1:]]


# test
@round_result(3)
def foo():
    d = {'A': {'B': 1.222, 'C': 3.333}, 'D': 2.444,
         'E': [1.111, 2.222,
               {'FF':
                    [1.111, 53.11,
                     {'GDF': [12414.222, 334.22]}]}]}

    return d

# ------------------- Check market time ----------------------

def checkMarketTime(now = None):
    import datetime, pytz
    import pandas_market_calendars as mcal

    tz = pytz.timezone('US/Eastern')
    if not now:
        now = datetime.datetime.now(tz)
    nyse = mcal.get_calendar('NYSE')
    market_days = nyse.valid_days(start_date=now, end_date=now,tz=tz)
    openTime = datetime.time(hour = 9, minute = 30, second = 0)
    closeTime = datetime.time(hour = 16, minute = 0, second = 0)
    # If not market day
    if now.strftime('%Y-%m-%d') not in market_days:
        return False
    # If before 09:30 or after 16:00 (not include pre-market and after-market)
    if (now.time() < openTime) or (now.time() > closeTime):
        return False

    return True

# ---------------------- SQL Related... ----------------------

def sql_to_dict(result_proxy) -> List[dict]:
    return [dict(row) for row in result_proxy]


@contextmanager
def mysql_session_scope():
    session = scoped_session(mysql_session_factory)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
