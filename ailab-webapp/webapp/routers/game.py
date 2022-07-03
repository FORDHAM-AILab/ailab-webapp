import json
import logging
import traceback
from typing import List

from fastapi import APIRouter, Depends
from fastapi.requests import Request
from datetime import datetime, timedelta

from models.Portfolio import Portfolio
from .. import helpers, CONSTS
from ..CONSTS import TIME_ZONE
from ..data.stock import get_hist_stock_price
from ..webapp_models.db_models import InternalUser
import uuid
from collections import defaultdict
from ..auth import schemes as auth_schemes
from ..helpers import format_pct, format_digit, format_currency, round_result
from ..webapp_models.generic_models import ResultResponse

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
    with helpers.mysql_session_scope() as session:
        session.execute(
            f"""INSERT INTO game_rm_account (user_id, net_account_value,market_value,cash_balance, pl,pl_percent,
                updated_at, created_at) 
                VALUES ('{internal_user.internal_sub_id}',{CONSTS.GAME_RM_NOTIONAL},0,{CONSTS.GAME_RM_NOTIONAL},0,0,'{current_time}',
                        '{current_time}')""")

    return ResultResponse(status=0, message=f"Create user: {internal_user.username} for RM game",
                          date_done=str(current_time))


@router.post("/rm_game/update_portfolio")
async def update_portfolio(request: Request,
                           internal_user: InternalUser = Depends(access_token_cookie_scheme)) -> ResultResponse:
    """
        :param request: Request object that contains transaction infos, e.g.: {"transactions":{"AAPL":10, "TSLA":4}}
        :param internal_user:
        :return:
    """

    try:

        # ---------------------Market is open-----------------------#
        if helpers.checkMarketTime():

            with helpers.mysql_session_scope() as session:
                result_current = session.execute(f"""SELECT * FROM game_rm_account WHERE 
                                                     user_id = '{internal_user.internal_sub_id}' """)
                if result_current.rowcount == 0:
                    raise Exception(f'User: {internal_user.username} not registered for the game')
                result_current = helpers.sql_to_dict(result_current)

            new_transaction = await request.json()
            new_transactions = json.loads(new_transaction['transactions'])
            net_account_value = float(result_current[0]['net_account_value'])
            market_value = float(result_current[0]['market_value'])
            cash_balance = float(result_current[0]['cash_balance'])
            # leverage allowed: cash can lend the same value as itself, stock can lend 0.8 times of its value
            buying_power = market_value * 0.8 + (cash_balance * 2 if cash_balance > 0 else cash_balance)
            current_time = datetime.now(CONSTS.TIME_ZONE).isoformat()

            # if the user didn't have any transaction before, i.e. current_shares dict is None, create empty defaultdict
            if result_current[0]['current_shares'] is None:
                new_shares = defaultdict(lambda: 0)
            # otherwise, modify from current's
            else:
                new_shares = defaultdict(lambda: 0, json.loads(result_current[0]['current_shares']))
            for ticker, n_shares in new_transactions.items():
                new_shares[ticker] += n_shares
            new_shares = dict(new_shares)
            # get today's price:
            new_prices = get_hist_stock_price(list(new_shares.keys()), current_time, current_time).to_dict(orient='records')[0]
            # deduct from the current balance
            for ticker, shares in new_transactions.items():
                # insufficient buying power
                if buying_power < (shares * new_prices[ticker]):  # Leverage allowed
                    raise Exception(f"Insufficient buying power for transaction: {'Buy' if shares > 0 else 'Sell'} "
                                    f"{ticker} {abs(shares)} shares")
                else:
                    if helpers.checkMarketTime():
                        cash_balance -= shares * new_prices[ticker]

            # get the historical price table for one year for VaR
            annual_price = get_hist_stock_price(list(new_shares.keys()),
                                                datetime.now(CONSTS.TIME_ZONE) - timedelta(days=365), datetime.now(CONSTS.TIME_ZONE))
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
                                                           , net_account_value = {cash_balance + market_value}
                                                           , market_value = {market_value}
                                                           , cash_balance = {cash_balance}
                                                           , hist_var = {hist_var}
                                                           , p_var = {p_var}
                                                           , monte_carlo_var = {monte_carlo_var}
                                                           , pl = {cash_balance + market_value - CONSTS.GAME_RM_NOTIONAL}
                                                           , pl_percent = {round((cash_balance + market_value - CONSTS.GAME_RM_NOTIONAL) / 100, 2)}
                                                           , current_shares = '{json.dumps(new_shares)}'
                                                           WHERE user_id = '{internal_user.internal_sub_id}'""")

                # record new trades request
                for ticker, shares in new_transactions.items():
                    session.execute(f"""INSERT INTO game_rm_transactions VALUES ('{uuid.uuid4()}', 
                                        '{internal_user.internal_sub_id}','{current_time}', '{ticker}', {shares},'completed')""")

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
                        average_price = round(
                            (average_price * quantity + new_price * (new_shares[ticker] - quantity)) / shares, 2)

                        open_pl = new_market_value - average_price * shares
                        session.execute(f"""UPDATE game_rm_portfolio SET market_value = {new_market_value}
                                                                        , quantity = {shares}
                                                                        , open_pl = {open_pl}
                                                                        , open_pl_percent = {round(open_pl / 100, 2)}
                                                                        , last_price = {new_price}
                                                                        , average_price = {average_price}
                                                                        WHERE user_id = '{internal_user.internal_sub_id}'
                                                                        AND ticker = '{ticker}'""")

        # ---------------------Market is close-----------------------#
        else:
            return ResultResponse(status=-2, message="Market Close",
                                  date_done=str(datetime.now(CONSTS.TIME_ZONE).isoformat()))
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
    with helpers.mysql_session_scope() as session:
        result = session.execute(
            f"""SELECT transaction_id, ticker, shares, price, transaction_time FROM game_rm_transactions WHERE user_id='{internal_user.internal_sub_id}' 
                ORDER BY transaction_time DESC""")
        result = helpers.sql_to_dict(result)
        result_to_return = []
        for row in result:
            print(row)
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
    with helpers.mysql_session_scope() as session:
        result = session.execute(
            f"""SELECT username, net_account_value FROM game_rm_account LEFT JOIN users 
                ON game_rm_account.user_id = users.internal_sub_id ORDER BY {by} DESC""")
        result = helpers.sql_to_dict(result)

        for idx, res in enumerate(result):
            res['Rank'] = idx

    return ResultResponse(status=0, result=result, date_done=str(datetime.now(TIME_ZONE).isoformat()))


@router.post("/rm_game/reset_game")
async def reset_game(internal_user: InternalUser):
    with helpers.mysql_session_scope() as session:
        session.execute(f"""DELETE FROM game_rm_account WHERE user_id = '{internal_user.internal_sub_id}'""")
        session.execute(f"""DELETE FROM game_rm_transactions WHERE user_id = '{internal_user.internal_sub_id}'""")
        session.execute(f"""DELETE FROM game_rm_portfolio WHERE user_id = '{internal_user.internal_sub_id}'""")
    return ResultResponse(status=0, message=f"Reset user: {internal_user.username} for RM game",
                          date_done=str(datetime.now(TIME_ZONE).isoformat()))


@round_result(CONSTS.PRICE_DECIMAL)
def _get_user_account_info(internal_user: InternalUser) -> dict:
    with helpers.mysql_session_scope() as session:
        result = session.execute(
            f"""SELECT * from game_rm_account WHERE user_id = "{internal_user.internal_sub_id}" """)
        result = helpers.sql_to_dict(result)

    if len(result) == 0:
        raise Exception(f'User: {internal_user.username} not registered for the game')
    return result[0]


@router.get("/rm_game/get_user_account_info")
async def get_user_account_info(internal_user: InternalUser = Depends(access_token_cookie_scheme)):
    result = _get_user_account_info(internal_user)
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
def _get_user_position(internal_user: InternalUser) -> List[dict]:
    with helpers.mysql_session_scope() as session:
        result = session.execute(
            f"""SELECT * from game_rm_portfolio WHERE user_id = "{internal_user.internal_sub_id}" """)
        result = helpers.sql_to_dict(result)

    return result


@router.get("/rm_game/get_user_position")
async def get_user_position(internal_user: InternalUser = Depends(access_token_cookie_scheme)):
    result = _get_user_position(internal_user)
    rows = []

    for row in result:
        modified_row = {'Ticker': row['ticker'],
                        'Market Value': format_currency(row['market_value']),
                        'Quantity': row['quantity'],
                        'Open P&L': format_currency(row['open_pl']),
                        'Open P&L %': format_pct(row['open_pl_percent'] * 100),
                        'Last Price': format_currency(row['last_price']),
                        'AVG Price': format_currency(row['average_price'])}
        rows.append(modified_row)

    return ResultResponse(status=0, result=rows,
                          message=f"Found user: {internal_user.username}'s current account info",
                          date_done=str(datetime.now(TIME_ZONE).isoformat()))
