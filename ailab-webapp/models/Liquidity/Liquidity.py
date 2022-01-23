# -*- coding: utf-8 -*-
"""
Created on Fri Aug 10 10:26:51 2018

@author: William Huang
"""

from scipy.special import comb
from models.Binomial.BinomialTree import BinomialTree
from scipy.stats import norm
from scipy.optimize import root
from scipy.optimize import brentq
import numpy as np
import logging
import math


def BSPricer(s, k, rf, div, vol, T, pcFlag):
    # logging.debug(f'{s},{k}')
    d1 = (math.log(s / k) + (rf - div + vol * vol / 2) * T) / (vol * math.sqrt(T))
    d2 = d1 - vol * math.sqrt(T)

    n1 = norm.cdf(d1 * pcFlag, 0, 1)
    n2 = norm.cdf(d2 * pcFlag, 0, 1)
    result = s * math.exp(-div * T) * pcFlag * n1 - k * math.exp(-rf * T) * pcFlag * n2
    # logging.debug(f'-> {result}')

    return result


def BSPricer_calibrate(s, rf, div, vol, T, pcFlag, adjustmentFactor, equityMarket) -> float:
    # logging.debug(f'{s},{k}')
    d1 = (math.log(s / s * adjustmentFactor) + (rf - div + vol * vol / 2) * T) / (vol * math.sqrt(T))
    d2 = d1 - vol * math.sqrt(T)

    n1 = norm.cdf(d1 * pcFlag, 0, 1)
    n2 = norm.cdf(d2 * pcFlag, 0, 1)
    result = s * math.exp(-div * T) * pcFlag * n1 - s * adjustmentFactor * math.exp(
        -rf * T) * pcFlag * n2 - equityMarket
    # logging.debug(f'-> {result}')

    return result


class Liquidity(BinomialTree):
    def __init__(self, k=1, rf=0.05, steps=50, vol=0.2, ttm=1):
        if steps % (k + 1) != 0:
            raise Exception("Invalid rebalance frequency", k)
        self.k = k
        self.n1 = steps / (k + 1)
        super(Liquidity, self).__init__(rf, steps, vol, ttm)

    def reverseSharpRatio(self, targetProb, rf, vol, t, steps):
        dt = t / steps
        u = math.exp(math.sqrt(dt) * vol)
        d = 1 / u
        emut = targetProb * (u - d) + d
        mu = math.log(emut) / dt
        return (mu - rf) / vol

    def calculateBeta(self, stk0, opt0, endOptPrice, n):
        # Initialize
        sum_opt_price, sum_opt_return, sum_stk_return, crss_opt_stk, sum2_stk_return = 0, 0, 0, 0, 0

        for j in range(n):
            stk_return = math.pow(self.up, j) * math.pow(self.dn, (n - j))
            opt_return = endOptPrice[j] / opt0
            prob = comb(n, j) * math.pow(self.pr, j) * math.pow((1 - self.pr), (n - j))
            sum_opt_price = sum_opt_price + endOptPrice[j] * prob
            sum_opt_return = sum_opt_return + opt_return * prob
            sum_stk_return = sum_stk_return + stk_return * prob
            crss_opt_stk = crss_opt_stk + opt_return * stk_return * prob
            sum2_stk_return = sum2_stk_return + stk_return * stk_return * prob
        cov = crss_opt_stk - sum_stk_return * sum_opt_return
        var_stk_return = sum2_stk_return - sum_stk_return * sum_stk_return
        # Equation 22 in paper
        beta = cov / var_stk_return

        return beta

    def liquidPrice(self, s0, k1, k2, t1, t2, div):
        self.Tree(s0=s0, func=lambda x: BSPricer(k2, self.rf, div, self.vol, t2 - t1, 1))
        return self.expectedResult(func=BinomialTree.optionExercise(self, k1))

    def getLiquidPrice(self, k1, k2, t1, t2, div, marketCap):
        return root(lambda x: self.liquidPrice(x, k1, k2, t1, t2, div) - marketCap, np.array([20])).x[0]

    def illquidPrice(self, a0, k, ttm, rf, sharpR, sigma):
        # This is a function of closed form of illiquidPrice in Pro. Chen's paper Valuing a Liquidity Discount
        # inputs are:
        # k is 1/2 long term debt + short term debt;
        # a0 is initial firm asset value
        # ttm is time to maturity
        # rf is risk free rate
        # sharpR is sharp ratio
        # sigma is volatility

        mu = rf + sharpR * sigma
        dp = (math.log(a0 / k) + (mu + 0.5 * sigma * sigma) * ttm) / (sigma * math.sqrt(ttm))
        dd = dp - sigma * math.sqrt(ttm)
        dpp = dp + sigma * math.sqrt(ttm)

        EXT = a0 * math.exp(mu * ttm) * norm.cdf(dp) - k * norm.cdf(dd)
        EVT = a0 * math.exp(mu * ttm)
        EXTVT = math.pow(a0, 2) * math.exp((2 * mu + sigma * sigma) * ttm) * norm.cdf(dpp) - k * a0 * math.exp(
            mu * ttm) * norm.cdf(dp)

        cov = EXTVT - EXT * EVT
        var = math.pow(a0, 2) * math.exp(2 * mu * ttm) * (math.exp(sigma * sigma * ttm) - 1)

        beta = cov / var

        result = math.exp(-rf * ttm) * (EXT - beta * (EVT - a0 * math.exp(rf * ttm)))

        return result

    def calibrateWealth(self, adjustmentFactor, rf, div, vol, T, pcFlag, equityMarket):
        return brentq(BSPricer_calibrate,
                      0.01, 200 * equityMarket,
                      args=(rf, div, vol, T, pcFlag, adjustmentFactor, equityMarket))

# return root(lambda w: Liquidity.BSPricer(w, w * adjustmentFactor, rf, div, vol, T, pcFlag) - equityMarket,
#			[equityMarket]).x[0]