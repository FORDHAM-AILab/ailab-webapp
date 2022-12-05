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

    def bs(self):
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

        d1, d2 = self.d1(), self.d2()

        n1 = norm.cdf(d1 * self.pcFlag, 0, 1)
        n2 = norm.cdf(d2 * self.pcFlag, 0, 1)
        result = self.s * math.exp(-self.div * self.T) * self.pcFlag * n1 - \
                 self.k * math.exp(-self.rf * self.T) * self.pcFlag * n2
        return result

    def binomial_tree(self, N):

        # upward factor
        u = np.exp(self.vol * np.sqrt(self.T / N))
        # downward factor
        d = np.exp(-self.vol * np.sqrt(self.T / N))
        # prob for price rise
        pu = ((np.exp(self.rf * self.T / N)) - d) / (u - d)
        # prob for price down
        pd = 1 - pu
        # discount rate
        disc = np.exp(-self.rf * self.T / N)

        St = [0] * (N + 1)
        C = [0] * (N + 1)

        St[0] = self.s * d ** N

        for j in range(1, N + 1):
            St[j] = St[j - 1] * u / d

        for j in range(1, N + 1):
            if self.pcFlag == -1:
                C[j] = max(self.k - St[j], 0)
            else:
                C[j] = max(St[j] - self.k, 0)
        for i in range(N, 0, -1):
            for j in range(0, i):
                C[j] = disc * (pu * C[j + 1] + pd * C[j])

        return C[0]

