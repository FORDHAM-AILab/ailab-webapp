import pandas as pd
import numpy as np


def calc_returns(df, method=None):
    if method == 'simple':
        returns = df - df.shift(1)
    elif method == 'log':
        returns = np.log(df) - np.log(df.shift(1))
    else:
        returns = df.pct_change()
    return returns.fillna(method='backfill')


def covariance_matrix(df: pd.DataFrame, method=None, **kwargs) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Data must be a pandas dataframe.")

    if method is None:
        return df.cov()
    elif method == 'ewma' or method == 'ewm':
        ewa = df.ewm(**kwargs)
        index = ewa.iloc[-1, :].name[0]
        cov = ewa.cov().loc[(index, slice(None)), :]
        return cov


