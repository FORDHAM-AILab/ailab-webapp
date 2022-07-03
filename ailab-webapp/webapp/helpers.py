import traceback
from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Union

import pandas as pd
import numpy as np
from functools import wraps
import copy
from contextlib import contextmanager

from sqlalchemy.orm import scoped_session
from . import mysql_session_factory, CONSTS
from .CONSTS import TIME_ZONE
from .webapp_models.generic_models import ResultResponse


def standard_response(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        try:
            rst = function(*args, **kwargs)
        except Exception as e:
            return ResultResponse(status='-1', message=f"An exception occurred: {str(e)}",
                                  debug=traceback.format_exc(),
                                  date_done=str(datetime.now(TIME_ZONE).isoformat()))
        else:
            return ResultResponse(status='0', message="Succeed", result=rst,
                                  date_done=str(datetime.now(TIME_ZONE).isoformat()))

    return wrapper


def format_currency(num: Optional[Union[int, float]]):
    return None if num is None or np.isnan(num) or np.isinf(num) else f'${num:,}'


def format_pct(num: Optional[Union[int, float]], decimal=CONSTS.PRICE_DECIMAL):
    return None if num is None or np.isnan(num) or np.isinf(num) else f"%.{decimal}f%%" % num


def format_digit(num: float, suffix: List[str] = 'K'):
    """
    Helper function, replace 0s with K, M, B etc.
    """
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    # add more suffixes if you need them
    return '%.2f%s' % (num, suffix[magnitude])


def round_digit(x, decimal: int = 0):
    return x if (isinstance(x, (str, int, datetime)) or (x is None)) \
        else round(float(x), decimal) if isinstance(x, Decimal) \
        else None if (np.isnan(x) or np.isinf(x)) \
        else int(round(x) + 0) if isinstance(x, float) and decimal == 0 \
        else round(x, decimal) if isinstance(x, float) and decimal != 0 \
        else x


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
    Accept float,dict,list and str
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
