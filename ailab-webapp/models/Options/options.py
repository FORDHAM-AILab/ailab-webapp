import pandas as pd
import numpy as np
import math
from scipy.stats import norm
from scipy.optimize import root
from scipy.optimize import brentq


class Options:
    def __init__(self, s, k, rf, div, vol, T, pcFlag):
        """

        :param s: spot price
        :param k: strike price
        :param rf: risk-free rate
        :param div:
        :param vol: volatility of the underlying asset
        :param T: time to maturity
        :param pcFlag: 1 for call -1 for put
        """
        self.s = s
        self.k = k
        self.rf = rf
        self.div = div
        self.vol = vol
        self.T = T
        self.pcFlag = pcFlag

    def d1(self):
        return (math.log(self.s / self.k) + (self.rf - self.div + self.vol * self.vol / 2) * self.T) \
               / (self.vol * math.sqrt(self.T))

    def d2(self):
        return self.d1() - self.vol * math.sqrt(self.T)

def bs(s, k, rf, div, vol, T, pcFlag):
    """

    :param s: spot price
    :param k: strike price
    :param rf: risk-free rate
    :param div:
    :param vol: volatility of the underlying asset
    :param T: time to maturity
    :param pcFlag: 1 for call -1 for put
    :return:
    """
    d1 = (math.log(s / k) + (rf - div + vol * vol / 2) * T) / (vol * math.sqrt(T))
    d2 = d1 - vol * math.sqrt(T)

    n1 = norm.cdf(d1 * pcFlag, 0, 1)
    n2 = norm.cdf(d2 * pcFlag, 0, 1)
    result = s * math.exp(-div * T) * pcFlag * n1 - k * math.exp(-rf * T) * pcFlag * n2
    return result


def binomial_tree(S, K, T, r, sigma, N, is_call):
    """

    :param S: underlying asset price
    :param K: strike price
    :param T: time to maturity(in years)
    :param r: risk-free rate
    :param sigma: volatility
    :param N: number of steps for binomial tree
    :param is_call
    :return: option price
    """

    # upward factor
    u = np.exp(sigma * np.sqrt(T / N))
    # downward factor
    d = np.exp(-sigma * np.sqrt(T / N))
    # prob for price rise
    pu = ((np.exp(r * T / N)) - d) / (u - d)
    # prob for price down
    pd = 1 - pu
    # discount rate
    disc = np.exp(-r * T / N)

    St = [0] * (N + 1)
    C = [0] * (N + 1)

    St[0] = S * d ** N

    for j in range(1, N + 1):
        St[j] = St[j - 1] * u / d

    for j in range(1, N + 1):
        if not is_call:
            C[j] = max(K - St[j], 0)
        else:
            C[j] = max(St[j] - K, 0)
    for i in range(N, 0, -1):
        for j in range(0, i):
            C[j] = disc * (pu * C[j + 1] + pd * C[j])

    return C[0]
