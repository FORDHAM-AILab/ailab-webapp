import numpy as np
import pandas as pd
from typing import List, Tuple, Optional, Union
from models.utils import *
from scipy.stats import norm
import random


class Portfolio:
    def __init__(self, df: pd.DataFrame, header: Optional[List[str]] = None, weights: Optional[np.array] = None, port_value: Optional[float] = 1):

        if not isinstance(df, pd.DataFrame): # Is the df stock price?
            raise ValueError("Data must be a pandas dataframe.")
        self.df = df

        if port_value is not None and port_value > 0:
            self.df *= port_value # What is port_value? 

        if header is not None and len(header) == len(df.columns):
            self.df.columns = header

        date_cols = [col for col in df.columns if 'date' in col.lower()]
        if len(date_cols) > 0:
            self.df.set_index(date_cols)
            self.df.drop(date_cols, axis=1, inplace=True)
        if weights is not None and len(weights) == len(self.df.columns):
            self.weights = weights
        else:
            self.weights = np.full(len(self.df.columns), 1 / len(self.df.columns))
        self.df = self.df.astype(float)
        self.df = self.df * self.weights
        self.df['Portfolio'] = self.df.sum(axis=1)

        self.log_return = calc_returns(self.df, method='simple') # why use 'simple' instead of 'log' method?
        self.log_return_cov = covariance_matrix(self.log_return)

    def basic_info(self):
        basic_info = self.log_return.describe()
        basic_info.loc['Annualized Return'] = basic_info.loc['mean'] * 252
        return basic_info

    def sharpe_r(self, rf=0) -> pd.DataFrame:
        return_pct = calc_returns(self.df, method='pct')
        sharpe_ratio = (return_pct.mean() - rf) / return_pct.std()

        return sharpe_ratio

    def pvar(self, ci=0.95, alpha=None):
        if alpha is None:
            return abs(norm.ppf(ci) * np.sqrt(self.log_return['Portfolio'].var()))
        else:
            ew = (1 - alpha) ** np.arange(len(self.log_return))[::-1]
            ew_return = ew * self.log_return['Portfolio']
            return abs(norm.ppf(ci) * np.sqrt(ew_return.var()))

    def hvar(self, level=5):
        return abs(np.percentile(self.log_return['Portfolio'], float(level), interpolation="nearest"))

    def monte_carlo_var(self, level=5, n=10000):
        port_return_mean = self.log_return['Portfolio'].mean()
        port_return_std = self.log_return['Portfolio'].std()
        mc_return = np.random.normal(loc=port_return_mean, scale=port_return_std, size=n)
        return abs(np.percentile(mc_return, level, interpolation="nearest"))

    def mdd(self, window=252):
        """

        :param mode: 'rolling' for rolling mdd, 'single' for one single mdd for the whole portfolio
        :param window: rolling window size. Default 252 for annually MDD
        :return: return a df of rolling Maximum Drawdown
        """

        returns = calc_returns(self.df)
        roll_max = returns.rolling(window, min_periods=1).max()
        daily_dd = returns / roll_max - 1
        max_daily_dd = daily_dd.rolling(window, min_periods=1).min()
        return max_daily_dd

    def optimization(self, method='Markowitz'): # modify method from 'Sharpe' to 'Markowitz'
        pass


if __name__ == '__main__':
    # df = pd.read_csv('/Users/xuanmingcui/Documents/projects/ailab-webapp/ailab-webapp/models/VaR/Data/portfolio.csv')
    df = pd.read_csv('/Users/xuanmingcui/Downloads/data.csv')
    p = Portfolio(df)
    print(p.pvar(0.05), p.hvar(), p.monte_carlo_var())
    print(norm.ppf(0.95))
