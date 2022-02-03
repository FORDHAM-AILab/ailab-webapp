import pandas as pd
import numpy as np
from yahoo_fin import options
from typing import List, Optional, Union
from datetime import datetime


def get_options_expiration_date(ticker: str) -> List:
    return options.get_expiration_dates(ticker)


def get_options_data(ticker: str, date: Union[str, datetime] = None, call: bool = True) -> pd.DataFrame:
    if call:
        df = options.get_calls(ticker=ticker, date=date)
    else:
        df = options.get_puts(ticker=ticker, date=date)
    return df.to_json(orient='records')


