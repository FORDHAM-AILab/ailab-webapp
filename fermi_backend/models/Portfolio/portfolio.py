import numpy as np
import pandas as pd
from typing import List, Tuple, Optional, Union
from fermi_backend.models.utils import *
from scipy.stats import norm
import random
import scipy.optimize as solver

class Portfolio:
    # read dataframe
    def __init__(self, df: pd.DataFrame, header: Optional[List[str]] = None, weights: Optional[np.array] = None):

        # Suppose df is stock price (index is date; columns are tickers)
        if not isinstance(df, pd.DataFrame): 
            raise ValueError("Data must be a pandas dataframe.")
        self.df = df
        
        # read header (may remove?)
        if header is not None and len(header) == len(df.columns):
            self.df.columns = header
            
        # set 'date' column as dataframe index
        date_cols = [col for col in df.columns if 'date' in col.lower()]
        if len(date_cols) > 0:
            self.df.set_index(date_cols)
            self.df.drop(date_cols, axis=1, inplace=True)
        
        # read weight; null then input average weight
        if weights is not None and len(weights) == len(self.df.columns):
            self.weights = weights
        else:
            self.weights = np.full(len(self.df.columns), 1 / len(self.df.columns))

        # calculate log return dataframe
        self.log_return = calc_returns(self.df, method='log') 
        self.log_return = self.log_return.dropna(inplace=False)
        self.portfolio_return = (self.log_return * self.weights).sum(axis=1)


    # calculate annualized return for each stock
    def basic_info(self):
        basic_info = self.log_return.describe()
        basic_info.loc['Annualized Return'] = basic_info.loc['mean'] * 252
        return basic_info

    # calculate sharpe ratio (pass)
    def sharpe_r(self, rf=0) -> pd.DataFrame:
        return_pct = calc_returns(self.log_return, method='pct')
        sharpe_ratio = (return_pct.mean() - rf) / return_pct.std()

        return sharpe_ratio

    def pvar(self, ci=0.95, alpha=0):
        ew = (1 - alpha) ** np.arange(len(self.log_return))[::-1]
        ew_return = ew * self.portfolio_return
        return abs(norm.ppf(ci) * np.sqrt(ew_return.var()))

    # pass
    def hvar(self, level=5):
        return abs(np.percentile(self.portfolio_return, float(level), interpolation="nearest"))

    # pass
    def monte_carlo_var(self, level=5, n=10000):
        port_return_mean = self.portfolio_return.mean()
        port_return_std = self.portfolio_return.std()
        mc_return = np.random.normal(loc=port_return_mean, scale=port_return_std, size=n)
        return abs(np.percentile(mc_return, level, interpolation="nearest"))

    # calculate max-drawdown
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

    def optimization(self, method='Markowitz', given_return: Optional[float] = None): # modify method from 'Sharpe' to 'Markowitz'
        # calculate annualized return and covariance matrix
        mean = self.log_return.mean()*252
        cov = self.log_return.cov()*252
        # read given return
        if given_return is None:
            given_return = (self.log_return.mean()*252).mean()
        else:
            given_return = given_return

        if method == 'Markowitz':
            random.seed(8)
            x0 = self.weights
            constraints = [{'type': 'eq', 'fun': lambda x: sum(x) - 1},
                           {'type': 'eq', 'fun': lambda x: sum(x * mean) - given_return}]
            bounds = tuple((0, 1) for x in range(len(self.log_return.columns)))

            def std_weight(weight):
                var = np.dot(weight, cov)
                var = np.dot(var, weight.T)
                std = np.sqrt(var)
                return std

            outcome = solver.minimize(std_weight, x0=x0, constraints=constraints, bounds=bounds)
            return outcome["x"]

        elif method == 'Sharpe':
            pass


if __name__ == '__main__':
    # df = pd.read_csv('/Users/xuanmingcui/Documents/projects/fermi_backend/fermi_backend/webapp_models/VaR/Data/portfolio.csv')
    from webapp.data.stock import get_hist_stock_price
    df = get_hist_stock_price(['AAPL', 'TSLA'], start_date='2022-01-01', end_date='2022-02-15')
    p = Portfolio(df)
    print(p.pvar(0.05), p.hvar(), p.monte_carlo_var())
    print(p.optimization())
