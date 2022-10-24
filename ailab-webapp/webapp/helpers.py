import traceback
from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Union

import pandas as pd
import numpy as np
from functools import wraps
import copy
from contextlib import asynccontextmanager

from sqlalchemy.orm import scoped_session
from . import mysql_session_factory, CONSTS
from .CONSTS import TIME_ZONE
from .webapp_models.generic_models import ResultResponse
import asyncio
import logging
from asyncio import ensure_future
from functools import wraps
from traceback import format_exception
from typing import Any, Callable, Coroutine, Optional, Union

from starlette.concurrency import run_in_threadpool

NoArgsNoReturnFuncT = Callable[[], None]
NoArgsNoReturnAsyncFuncT = Callable[[], Coroutine[Any, Any, None]]
NoArgsNoReturnDecorator = Callable[[Union[NoArgsNoReturnFuncT, NoArgsNoReturnAsyncFuncT]], NoArgsNoReturnAsyncFuncT]

def sqlquote(value):
    """Naive SQL quoting

    All values except NULL are returned as SQL strings in single quotes,
    with any embedded quotes doubled.

    """
    if value is None:
        return 'NULL'
    return "'{}'".format(str(value).replace("'", "''"))


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


def format_currency(num: Optional[Union[int, float]], decimal=CONSTS.PRICE_DECIMAL):
    return None if num is None or np.isnan(num) or np.isinf(num) else str(round(num, decimal))


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
            if isinstance(result, float) or isinstance(result, Decimal):
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


# ------------------- Check market time ----------------------

def checkMarketTime(now=None):
    import datetime, pytz
    import pandas_market_calendars as mcal

    tz = pytz.timezone('US/Eastern')
    if not now:
        now = datetime.datetime.now(tz)
    nyse = mcal.get_calendar('NYSE')
    market_days = nyse.valid_days(start_date=now, end_date=now, tz=tz)
    openTime = datetime.time(hour=9, minute=30, second=0)
    closeTime = datetime.time(hour=16, minute=0, second=0)
    # If not market day
    if now.strftime('%Y-%m-%d') not in market_days:
        return False
    # If before 09:30 or after 16:00 (not include pre-market and after-market)
    if (now.time() < openTime) or (now.time() > closeTime):
        return False

    return True


# ---------------------- SQL Related... ----------------------

def sql_to_dict(result_proxy) -> List[dict]:
    if result_proxy is None:
        return []
    result = []
    for row in result_proxy:
        row_dict = dict(row)
        for k, v in row_dict.items():
            if type(v) == Decimal:
                row_dict[k] = float(v)
        result.append(row_dict)
    return result


@asynccontextmanager
async def mysql_session_scope():
    session = scoped_session(mysql_session_factory)
    try:
        yield session
        await session.commit()
    except:
        await session.rollback()
        raise
    finally:
        await session.close()

def schedule_task(
    *,
    scheduleHour: int,
    logger: Optional[logging.Logger] = None,
    raise_exceptions: bool = False,
    max_repetitions: Optional[int] = None,
) -> NoArgsNoReturnDecorator:
    """
    This function returns a decorator that modifies a function so it is periodically re-executed after its first call.

    The function it decorates should accept no arguments and return nothing. If necessary, this can be accomplished
    by using `functools.partial` or otherwise wrapping the target function prior to decoration.

    Parameters
    ----------
    seconds: float
        The number of seconds to wait between repeated calls
    wait_first: bool (default False)
        If True, the function will wait for a single period before the first call
    logger: Optional[logging.Logger] (default None)
        The logger to use to log any exceptions raised by calls to the decorated function.
        If not provided, exceptions will not be logged by this function (though they may be handled by the event loop).
    raise_exceptions: bool (default False)
        If True, errors raised by the decorated function will be raised to the event loop's exception handler.
        Note that if an error is raised, the repeated execution will stop.
        Otherwise, exceptions are just logged and the execution continues to repeat.
        See https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.set_exception_handler for more info.
    max_repetitions: Optional[int] (default None)
        The maximum number of times to call the repeated function. If `None`, the function is repeated forever.
    """

    def decorator(func: Union[NoArgsNoReturnAsyncFuncT, NoArgsNoReturnFuncT]) -> NoArgsNoReturnAsyncFuncT:
        """
        Converts the decorated function into a repeated, periodically-called version of itself.
        """
        is_coroutine = asyncio.iscoroutinefunction(func)

        @wraps(func)
        async def wrapped() -> None:
            repetitions = 0

            async def loop() -> None:
                nonlocal repetitions
                """ 
                if today + time > time.now(): 
                    await asyncio.sleep(t + time - time.now())
                else: 16:01   
                    await ASNYCIO.SLEEP(TIME.NOW() - (TODAY + TIME) + 24 * 60 * 60)
                """

                while max_repetitions is None or repetitions < max_repetitions:
                    try:
                        if is_coroutine:
                            await func()  # type: ignore
                            # time+tomorrow - time.now()
                        else:
                            await run_in_threadpool(func)
                            # time+tomorrow - time.now()
                        repetitions += 1
                    except Exception as exc:
                        if logger is not None:
                            formatted_exception = "".join(format_exception(type(exc), exc, exc.__traceback__))
                            logger.error(formatted_exception)
                        if raise_exceptions:
                            raise exc

                    # sleep until 4pm
                    import datetime
                    t = datetime.datetime.today()
                    future = datetime.datetime(t.year, t.month, t.day, 16, 0)
                    if t.hour >= scheduleHour:
                        future += datetime.timedelta(days=1)
                        await asyncio.sleep((future - t).total_seconds())
                    else:
                        await asyncio.sleep((future - t).total_seconds())
                    #await asyncio.sleep(seconds)

            ensure_future(loop())

        return wrapped

    return decorator
