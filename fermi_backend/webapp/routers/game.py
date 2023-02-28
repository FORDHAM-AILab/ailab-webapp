import json
import logging
import traceback
from typing import List, Union

import numpy as np
from fastapi import APIRouter, Depends
from fastapi.requests import Request
from datetime import datetime, timedelta

from fermi_backend.models.Portfolio import Portfolio
from .. import helpers, CONSTS
from ..CONSTS import TIME_ZONE
from ..data.stock import get_hist_stock_price, get_real_time_data
from ..webapp_models.db_models import InternalUser
import uuid
from collections import defaultdict
from ..auth import schemes as auth_schemes
from ..helpers import format_pct, format_digit, format_currency, round_result, schedule_task
from ..webapp_models.generic_models import ResultResponse
from fastapi_utils.tasks import repeat_every
from starlette.concurrency import run_in_threadpool
from ..CONSTS import PRICE_DECIMAL
import asyncio
from sqlalchemy import select

router = APIRouter(
    prefix="/game",
    tags=["game"]
)

csrf_token_redirect_cookie_scheme = auth_schemes.CSRFTokenRedirectCookieBearer()
auth_token_scheme = auth_schemes.AuthTokenBearer()
access_token_cookie_scheme = auth_schemes.AccessTokenCookieBearer()
logger = logging.getLogger(__name__)


@router.post("/rm_game/create_rm_game_user")
async def create_rm_game_user(internal_user: InternalUser = Depends(access_token_cookie_scheme)):
    current_time = datetime.now(TIME_ZONE).isoformat()
    async with helpers.mysql_session_scope() as session:
        await session.execute(
            f"""INSERT INTO game_rm_account (user_id, net_account_value,market_value,cash_balance, pl,pl_percent,
                updated_at, created_at) 
                VALUES ('{internal_user.internal_sub_id}',{CONSTS.GAME_RM_NOTIONAL},0,{CONSTS.GAME_RM_NOTIONAL},0,0,'{current_time}',
                        '{current_time}')""")

    return ResultResponse(status=0, message=f"Create user: {internal_user.username} for RM game",
                          date_done=str(current_time))


@router.post("/rm_game/update_portfolio")
async def update_portfolio(request: dict,
                           internal_user: InternalUser = Depends(access_token_cookie_scheme)) -> ResultResponse:
    """{"transactions":{"AAPL":10, "TSLA":4}}
        :param request: Request object that contains transaction infos, e.g.: {"transactions":{"AAPL":10, "TSLA":4}}
        :param internal_user:
        :return:
    """

    try:

        # ---------------------Market is close-----------------------#
        # if not helpers.checkMarketTime():
        #     return ResultResponse(status=-2, message="Market Close",
        #                           date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))

        # ---------------------Market is open-----------------------#

        async with helpers.mysql_session_scope() as session:
            current_account_info = await session.execute(f"""SELECT * FROM game_rm_account WHERE 
                                                 user_id = '{internal_user.internal_sub_id}' """)
            current_portfolio    = await session.execute(f"""SELECT * FROM game_rm_portfolio WHERE
                                                 user_id = '{internal_user.internal_sub_id}'""")
            if current_account_info.rowcount == 0:
                raise Exception(f'User: {internal_user.username} not registered for the game')
            current_account_info = helpers.sql_to_dict(current_account_info)
            current_portfolio    = helpers.sql_to_dict(current_portfolio)

        new_transaction = request#await request.json()
        new_transactions = json.loads(new_transaction['transactions'])
        net_account_value = float(current_account_info[0]['net_account_value'])
        market_value = float(current_account_info[0]['market_value'])
        cash_balance = float(current_account_info[0]['cash_balance'])
        # leverage allowed: cash can lend the same value as itself, stock can lend 0.8 times of its value
        buying_power = current_account_info[0]['buying_power']
        buying_power = float(buying_power) if buying_power else market_value * 0.8 + (cash_balance * 2 if cash_balance > 0 else cash_balance)
        current_time = datetime.now(CONSTS.TIME_ZONE).isoformat()

        current_shares = {row['ticker']: row['quantity'] for row in current_portfolio}
        current_average = {row['ticker']: float(row['average_price']) for row in current_portfolio}
        # if the user didn't have any transaction before, i.e. current_shares dict is None, create empty defaultdict
        if not current_shares:
            updated_shares = defaultdict(lambda: 0)
        # otherwise, modify from current's
        else:
            updated_shares = defaultdict(lambda: 0, current_shares)
        for ticker, n_shares in new_transactions.items():
            updated_shares[ticker] += n_shares
        updated_shares = dict(updated_shares)
        # get today's price:
        new_prices = get_real_time_data(list(updated_shares.keys())).to_dict(orient='records')[0]
        # deduct from the current balance
        for ticker, shares in new_transactions.items():
            # insufficient buying power
            if buying_power < (shares * new_prices[ticker]):  # Leverage allowed
                raise Exception(f"Insufficient buying power for transaction: {'Buy' if shares > 0 else 'Sell'} "
                                f"{ticker} {abs(shares)} shares. The whole transaction is cancelled")
            else:
                cash_balance -= shares * new_prices[ticker]

        # get the historical price table for one year for VaR
        annual_price = get_hist_stock_price(list(updated_shares.keys()),
                                            datetime.now(CONSTS.TIME_ZONE) - timedelta(days=365), datetime.now(CONSTS.TIME_ZONE))
        current_weights = [updated_shares[ticker] for ticker in annual_price.columns[:-1]]
        current_weights = [shares / sum(current_weights) for shares in current_weights]
        current_portfolio = Portfolio(df=annual_price, weights=current_weights)
        hist_var = current_portfolio.hvar()
        p_var = current_portfolio.pvar()
        monte_carlo_var = current_portfolio.monte_carlo_var()

        # get account market value
        market_value = 0
        for ticker, shares in updated_shares.items():
            market_value += new_prices[ticker] * shares

        # update buying_power
        buying_power = market_value * 0.8 + (cash_balance * 2 if cash_balance > 0 else cash_balance)
        account_pnl = cash_balance + market_value - CONSTS.GAME_RM_NOTIONAL
        # update account, transaction and portfolio info
        async with helpers.mysql_session_scope() as session:
            # update account
            await session.execute(f"""UPDATE game_rm_account SET updated_at = '{current_time}'
                                                       , net_account_value = {cash_balance + market_value}
                                                       , market_value = {market_value}
                                                       , cash_balance = {cash_balance}
                                                       , hist_var = {hist_var}
                                                       , p_var = {p_var}
                                                       , monte_carlo_var = {monte_carlo_var}
                                                       , pl = {round(account_pnl, PRICE_DECIMAL)}
                                                       , pl_percent = {round(account_pnl / CONSTS.GAME_RM_NOTIONAL, 4)}
                                                       , buying_power = {buying_power}
                                                       WHERE user_id = '{internal_user.internal_sub_id}'""")

            # record new trades request
            for ticker, shares in new_transactions.items():
                await session.execute(f"""INSERT INTO game_rm_transactions VALUES ('{uuid.uuid4()}', 
                                    '{internal_user.internal_sub_id}','{current_time}', '{ticker}', {shares}, {new_prices[ticker]}, 'COMPLETED')""")

            # ------- Post Calculations -------

            for ticker, change_shares in new_transactions.items():

                if ticker not in current_shares:
                    await session.execute(f"""INSERT INTO game_rm_portfolio VALUES ('{internal_user.internal_sub_id}', 
                                                    '{ticker}',{round(change_shares * new_prices[ticker], PRICE_DECIMAL)}, {change_shares}, 0,
                                                    {new_prices[ticker]},{new_prices[ticker]})""")
                else:
                    current_average_price = current_average[ticker]

                    new_price = new_prices[ticker]
                    new_market_value = change_shares * new_price
                    # update average_price
                    # If the updated shares == 0, then reset average cost price
                    # Only update average price when buying
                    if updated_shares[ticker] == 0:
                        new_average_price = 0
                    else:
                        if change_shares > 0:
                            new_average_price = round(
                                (current_average_price * current_shares[ticker] + new_price * change_shares) / updated_shares[ticker], PRICE_DECIMAL)
                        else:
                            new_average_price = current_average_price

                    open_pl = new_market_value - new_average_price * updated_shares[ticker]
                    await session.execute(f"""UPDATE game_rm_portfolio SET market_value = {new_market_value}
                                                                    , quantity = {updated_shares[ticker]}
                                                                    , open_pl = {open_pl}
                                                                    , last_price = {new_price}
                                                                    , average_price = {new_average_price}
                                                                    WHERE user_id = '{internal_user.internal_sub_id}'
                                                                    AND ticker = '{ticker}'""")


        return ResultResponse(status=0, message=f"Transaction succeed for user: {internal_user.username}",
                              date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))
    except Exception as e:
        return ResultResponse(status=-1, message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))


@router.get("/rm_game/get_transaction_history")
async def get_transaction_history(internal_user: InternalUser = Depends(access_token_cookie_scheme)):
    """
    return list of
    :param internal_user: InternalUser class
    :return:
    """
    async with helpers.mysql_session_scope() as session:
        result = await session.execute(
            f"""SELECT transaction_id, ticker, shares, price, transaction_time FROM game_rm_transactions WHERE user_id='{internal_user.internal_sub_id}' 
                ORDER BY transaction_time DESC""")
        result = helpers.sql_to_dict(result)
        result_to_return = []
        for row in result:
            new_row = {'Ticker': row['ticker'],
                       'Quantity': row['shares'],
                       'Price': row['price'],
                       'transaction_time': row['transaction_time']}
            result_to_return.append(new_row)
    return ResultResponse(status=0, result=result_to_return,
                          message=f"Transaction succeed for user: {internal_user.username}",
                          date_done=str(datetime.now(TIME_ZONE).isoformat()))


@router.get("/rm_game/rank_players_rm/{by}")
async def rank_players_rm(by: str = 'net_account_value'):
    """
    rank game participants by the given field
    :param by:
    :return:
    """
    async with helpers.mysql_session_scope() as session:
        today = datetime.now().strftime('%Y-%m-%d')
        result = await session.execute(
            f"""SELECT username, sharpe_ratio, net_account_value FROM game_rm_records LEFT JOIN users 
                ON game_rm_records.user_id = users.internal_sub_id 
                WHERE Date(date) = {today} 
                ORDER BY {by} DESC""")
        result = helpers.sql_to_dict(result)

        for idx, res in enumerate(result):
            res['Rank'] = idx+1

    return ResultResponse(status=0, result=result, date_done=str(datetime.now(TIME_ZONE).isoformat()))


@router.post("/rm_game/reset_game/{user_id}")
async def reset_game(user_id):
    async with helpers.mysql_session_scope() as session:
        await session.execute(f"""DELETE FROM game_rm_account WHERE user_id = '{user_id}'""")
        await session.execute(f"""DELETE FROM game_rm_transactions WHERE user_id = '{user_id}'""")
        await session.execute(f"""DELETE FROM game_rm_portfolio WHERE user_id = '{user_id}'""")
        username = await session.execute(f"""SELECT username FROM users WHERE internal_sub_id = {user_id} """)
    return ResultResponse(status=0, message=f"Reset user: {username} for RM game",
                          date_done=str(datetime.now(TIME_ZONE).isoformat()))


@round_result(CONSTS.PRICE_DECIMAL)
async def _get_user_account_info(internal_user: InternalUser) -> Union[dict, int]:
    async with helpers.mysql_session_scope() as session:
        result = await session.execute(
            f"""SELECT * from game_rm_account WHERE user_id = "{internal_user.internal_sub_id}" """)
        result = helpers.sql_to_dict(result)

    if len(result) == 0:
        return -1
    return result[0]


@router.get("/rm_game/get_user_account_info")
async def get_user_account_info(internal_user: InternalUser = Depends(access_token_cookie_scheme)):
    result = await _get_user_account_info(internal_user)
    if result == -1:
        return ResultResponse(status=-2, result=result,
                              message=f"User hasn't joined the game",
                              date_done=str(datetime.now(TIME_ZONE).isoformat()))
    return_result = {'net_account_value': format_currency(float(result['net_account_value'])),
                     'market_value': format_currency(float(result['market_value'])),
                     'cash_balance': format_currency(float(result['cash_balance'])),
                     'pl': format_currency(float(result['pl'])),
                     'pl_percent': format_pct(float(result['pl_percent']) * 100),
                     'hist_var': result['hist_var'],
                     'p_var': result['p_var'],
                     'monte_carlo_var': result['monte_carlo_var']}
    return ResultResponse(status=0, result=return_result,
                          message=f"Found user: {internal_user.username}'s current account info",
                          date_done=str(datetime.now(TIME_ZONE).isoformat()))


@round_result(CONSTS.PRICE_DECIMAL)
async def _get_user_position(internal_user: InternalUser) -> List[dict]:
    async with helpers.mysql_session_scope() as session:
        coroutine = await session.execute(
            f"""SELECT * from game_rm_portfolio WHERE user_id = "{internal_user.internal_sub_id}" """)
        result = helpers.sql_to_dict(coroutine)

    return result


@router.get("/rm_game/get_user_position")
async def get_user_position(internal_user: InternalUser = Depends(access_token_cookie_scheme)):
    result = await _get_user_position(internal_user)
    rows = []

    for row in result:
        modified_row = {'Ticker': row['ticker'],
                        'Market Value': format_currency(row['market_value']),
                        'Quantity': row['quantity'],
                        'Open P&L': format_currency(row['open_pl']),
                        'Last Price': format_currency(row['last_price']),
                        'AVG Price': format_currency(row['average_price'])}
        rows.append(modified_row)

    return ResultResponse(status=0, result=rows,
                          message=f"Found user: {internal_user.username}'s current account info",
                          date_done=str(datetime.now(TIME_ZONE).isoformat()))

@router.on_event('startup')
@schedule_task(scheduleHour=16)
@router.get("/rm_game/create_eod_records")
async def create_eod_records():
    """
    insert the end of day records for all game users
    """
    import pandas_market_calendars as mcal
    import datetime
    import yfinance as yf
    try:
        now = datetime.datetime.now(CONSTS.TIME_ZONE)
        nyse = mcal.get_calendar('NYSE')
        market_days = nyse.valid_days(start_date=now, end_date=now, tz=CONSTS.TIME_ZONE)
        refreshTime = datetime.time(hour=23, minute=59, second=59)
        closeTime = datetime.time(hour=16, minute=0, second=0)
        current_time = datetime.datetime.now(CONSTS.TIME_ZONE).isoformat()
        async with helpers.mysql_session_scope() as session:
            date_history = await session.execute(f"""SELECT date FROM game_rm_records""")
            if date_history is not None:
                date_history = helpers.sql_to_dict(date_history)
                date_history = [sub['date'].isoformat() for sub in date_history]
            else:
                date_history = []
        # If market day, no duplicated date, after 16:00 and before 23:59:59
        if (now.strftime('%Y-%m-%d') in market_days) and (now.strftime('%Y-%m-%d') not in date_history) and (now.time() > closeTime) and (now.time() < refreshTime):
            async with helpers.mysql_session_scope() as session:
                accounts = await session.execute(f"""SELECT * FROM game_rm_account;""")
                accounts = helpers.sql_to_dict(accounts)
                portfolio = await session.execute(f"""SELECT * FROM game_rm_portfolio;""")
                portfolio = helpers.sql_to_dict(portfolio)
                current_shares = {}
                for row in portfolio:
                    if row['user_id'] not in current_shares:
                        current_shares[row['user_id']] = {row['ticker']: row['quantity']}
                    else:
                        current_shares[row['user_id']][row['ticker']] = row['quantity']

                # iterate all users
                for i in range(len(accounts)):
                    account = accounts[i]
                    user_id = account['user_id']
                    cash_balance = account['cash_balance']
                    record = await run_in_threadpool(calculate_eod_records, current_shares=current_shares[user_id])
                    market_value = record['market_value']
                    net_account_value_today = cash_balance + market_value
                    net_account_value_history = await session.execute(f"""SELECT net_account_value FROM game_rm_records 
                                                                                WHERE user_id={user_id}""")
                    if net_account_value_history is not None:
                        net_account_value_history = helpers.sql_to_dict(net_account_value_history)
                        net_account_value_history = np.array([sub['net_account_value'] for sub in net_account_value_history])
                        sharpe_ratio = await run_in_threadpool(
                            calculate_eod_sharpe_ratio, net_account_value_today, net_account_value_history)
                    else:
                        sharpe_ratio = None

                    p_var = record['p_var']
                    # insert historical record
                    await session.execute(
                        f"""INSERT INTO game_rm_records (user_id, date, net_account_value,market_value,cash_balance, pl,pl_percent,
                                    p_var,current_shares,sharpe_ratio) 
                                    VALUES ('{user_id}',{current_time},'{net_account_value_today}',{market_value},'{cash_balance}',
                                    '{cash_balance + market_value - CONSTS.GAME_RM_NOTIONAL}',
                                    '{round((cash_balance + market_value - CONSTS.GAME_RM_NOTIONAL) / CONSTS.GAME_RM_NOTIONAL, PRICE_DECIMAL)}',
                                    '{p_var}',
                                    '{json.dumps(current_shares[user_id])}',
                                    '{sharpe_ratio}')""")



    except Exception as e:
        return ResultResponse(status=-1, message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.datetime.now(CONSTS.TIME_ZONE).isoformat()))



def calculate_eod_records(current_shares):
    """
    calculate end of day records for all game users
    """
    # get the historical price table for one year for VaR
    import yfinance as yf
    market_value = 0
    record = dict()
    for ticker, share in current_shares.items():
        ticker_yahoo = yf.Ticker(ticker)
        data = ticker_yahoo.history()
        last_price = data['Close'].iloc[-1]
        market_value += last_price * share

    annual_price = get_hist_stock_price(list(current_shares.keys()),
                                        datetime.now(CONSTS.TIME_ZONE) - timedelta(days=365),
                                        datetime.now(CONSTS.TIME_ZONE))
    current_weights = [current_shares[ticker] for ticker in annual_price.columns[:-1]]
    current_weights = [shares / sum(current_weights) for shares in current_weights]
    current_portfolio = Portfolio(df=annual_price, weights=current_weights)

    record['market_value'] = market_value
    record['p_var'] = current_portfolio.pvar()

    return record

def calculate_eod_sharpe_ratio(net_account_value_today,net_account_value_history):
    net_account_value_all = np.append(net_account_value_history,net_account_value_today)
    r = (net_account_value_today-CONSTS.GAME_RM_NOTIONAL)/CONSTS.GAME_RM_NOTIONAL
    sharpe_ratio = r/np.std(net_account_value_all)
    return sharpe_ratio


@router.get("/rm_game/get_historical_records")
async def get_historical_records(internal_user: InternalUser = Depends(access_token_cookie_scheme)):
    """
    get historical records for this internal_user
    """
    async with helpers.mysql_session_scope() as session:
        results = await session.execute(f"""SELECT * FROM game_rm_records WHERE internal_user.internal_sub_id""")
        results = helpers.sql_to_dict(results)
    return ResultResponse(status=0, result=results,
                          message=f"Transaction succeed for user: {internal_user.username}",
                          date_done=str(datetime.now(TIME_ZONE).isoformat()))


@router.get("/rm_game/get_historical_net_account_value")
async def get_historical_net_account_value(internal_user: InternalUser = Depends(access_token_cookie_scheme)):
    """
    get a summary of all users' records
    this is ONLY available for administrators (e.g. prof. Chen), i.e. we shall add an extra field to internal_user like 'type': student/GA/Administrator
    """
    async with helpers.mysql_session_scope() as session:
        results = await session.execute(f"""SELECT user_id,date,net_account_value FROM game_rm_records""")
        results = helpers.sql_to_dict(results)
    return ResultResponse(status=0, result=results,
                          message=f"Transaction succeed for user: {internal_user.username}",
                          date_done=str(datetime.now(TIME_ZONE).isoformat()))