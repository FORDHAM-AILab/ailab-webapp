# -*- coding: utf-8 -*-
"""
Created on Wed Aug  1 09:37:13 2018

@author: William Huang
"""

import math
import numpy as np
from scipy.special import comb


# np.set_printoptions(suppress=True)

class BinomialTree:

    def __init__(self, rf=0.05, steps=50, vol=0.2, ttm=2):
        if steps < 1:
            raise Exception("Invalid steps", steps)
        if vol < 0:
            raise Exception("Invalid volatility", vol)
        if ttm < 0:
            raise Exception("Invalid time to maturity", ttm)
        self.rf = rf
        self.steps = steps
        self.vol = vol
        self.ttm = ttm
        self.h = self.ttm / self.steps
        self.rh = math.exp(self.rf * self.h)
        self.up = math.exp(self.vol * math.sqrt(self.h))
        self.dn = 1 / self.up
        self.pr = (self.rh - self.dn) / (self.up - self.dn)

    def optionExercise(self, k, is_call=True):
        if is_call:
            return lambda x: x - k if (x - k > 0) else 0
        else:
            return lambda x: k - x if (k - x > 0) else 0

    def Tree(self, s0=100, func=None):
        self.s0 = s0
        self.options = np.zeros(self.steps + 1)
        for i in range(self.steps + 1):
            self.options[i] = s0 * math.pow(self.up, i) * math.pow(self.dn, self.currStep - i)
        if func is not None:
            self.options = np.array((list(map(func, self.options))))

    def reverse(self, Prob=False, func=None):
        last_step_opt = self.options.copy()
        self.currStep = self.currStep - 1
        for i in range(self.currStep + 1):
            self.options[i] = (last_step_opt[i + 1] * self.pr + last_step_opt[i] * (1 - self.pr)) / self.rh
        if func is not None:
            self.options = np.array((list(map(func, self.options))))

    def reverseToBeginning(self):
        while self.currStep > 0:
            self.reverse()
        return self.options[0]

    def calculateProb(self):
        self.probs = np.zeros(self.currStep + 1)
        for i in range(self.currStep + 1):
            self.probs[i] = comb(self.currStep, i) * math.pow(self.pr, i) * math.pow(1 - self.pr, self.currStep - i)
        self.probs.reshape(1, -1)

    def expectedResult(self, func=None):
        self.calculateProb()
        nodes = np.array((list(map(func, self.options.copy())))).reshape(-1, 1)
        return np.dot(self.probs, nodes)[0]


def binomial_tree(S, K, T, r, sigma, N, Option_type):
    """

    :param S: underlying asset price
    :param K: strike price
    :param T: time to maturity(in years)
    :param r: risk-free rate
    :param sigma: volatility
    :param N: number of steps for binomial tree
    :param Option_type: 'call' or 'put'. Default 'call'
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
        if Option_type == 'P':
            C[j] = max(K - St[j], 0)
        elif Option_type == 'C':
            C[j] = max(St[j] - K, 0)
    for i in range(N, 0, -1):
        for j in range(0, i):
            C[j] = disc * (pu * C[j + 1] + pd * C[j])

    return C[0]


if __name__ == '__main__':
    print(binomial_tree(100, 100, 1, 0.05, 0.2, 10, 'C'))
