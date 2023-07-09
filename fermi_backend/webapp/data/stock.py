import pandas as pd
import numpy as np
from requests_html import HTMLSession
from yahoo_fin import stock_info
from typing import List
import yfinance
from yahoo_fin.stock_info import _convert_to_numeric


def get_hist_stock_price(tickers, start_date, end_date):
    result = pd.DataFrame(columns=tickers)
    for ticker in tickers:
        result[ticker] = stock_info.get_data(ticker, start_date=start_date, end_date=end_date)['adjclose']

    result['date'] = result.index
    result['date'] = result['date'].dt.strftime('%Y-%m-%d')
    result.reset_index(inplace=True, drop=True)
    return result


def get_real_time_data(tickers):
    result = pd.DataFrame(columns=tickers)
    for ticker in tickers:
        result[ticker] = yfinance.download(ticker, period='3m',interval='1m')['Adj Close'].tail(1).values

    return result


def get_single_hist_price(ticker, start_date, end_date):
    result = stock_info.get_data(ticker, start_date=start_date, end_date=end_date)
    result.drop(columns=['ticker'], inplace=True)
    result['date'] = result.index
    result['date'] = result['date'].dt.strftime('%Y-%m-%d')
    result.reset_index(inplace=True, drop=True)
    return result


def get_analysis_info(ticker:str):
    return stock_info.get_analysts_info(ticker)


def _raw_get_daily_info(site):
    session = HTMLSession()

    resp = session.get(site)

    tables = pd.read_html(resp.html.raw_html)

    df = tables[0].copy()

    df.columns = tables[0].columns

    del df["52 Week Range"]

    df.dropna(inplace=True)
    df.reset_index(inplace=True)

    df["% Change"] = df["% Change"].map(lambda x: float(x.strip("%+").replace(",", "")))

    fields_to_change = [x for x in df.columns.tolist() if "Vol" in x \
                        or x == "Market Cap"]

    for field in fields_to_change:

        if type(df[field][0]) == str:
            df[field] = df[field].map(_convert_to_numeric)

    session.close()

    return df

def get_day_gainers(count: int = 100):

    return _raw_get_daily_info(f"https://finance.yahoo.com/gainers?offset=0&count={count}")


def get_day_losers(count: int = 100):

    return _raw_get_daily_info(f"https://finance.yahoo.com/losers?offset=0&count={count}")

def get_top_gainers(time_range):
    if time_range == 'daily':
        return get_day_gainers()
    else:
        pass


def get_top_losers(time_range):
    if time_range == 'daily':
        return get_day_losers()
    else:
        pass


def get_basic_stats(ticker):
    df = stock_info.get_stats(ticker)
    return df.to_json(orient='records')


def get_real_time_stock_price(tickers: list):
    return [stock_info.get_live_price(ticker) for ticker in tickers]


if __name__ == '__main__':
    print(get_real_time_stock_price(['AAPL']))