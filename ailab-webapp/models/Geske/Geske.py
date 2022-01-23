# -*- coding: utf-8 -*-
"""
Created on Wed Aug  1 11:05:32 2018

@author: kaihu
"""

from models.Binomial.BinomialTree import BinomialTree
import numpy as np
from scipy.optimize import root


class Geske(BinomialTree):
    'ttm and debt are array'

    def __init__(self, s0, ttm, Debt):
        super().__init__()
        self.s0 = s0
        self.ttm = ttm
        self.debt = Debt

    def geske(self):
        index = len(self.ttm) - 1
        self.Tree(s0=self.s0, func=BinomialTree.optionExercise(self, k=self.debt[index]))

        index -= 1
        while index >= 0:
            strike_step = int(self.ttm[index] / self.ttm[-1] * self.steps)
            while self.currStep > strike_step + 1:
                self.reverse()

            self.reverse(func=BinomialTree.optionExercise(self, k=self.debt[index]))

            index = index - 1

        return self.reverseToBeginning()

    def probOverride(self, optionValue):
        if optionValue > 0:
            return 1.0
        else:
            return 0.0

    def errorFunc(self, marketCap):
        return lambda x: self.geske() - marketCap

    def getAssetPrice(self, marketCap):
        result = root(self.errorFunc(marketCap=marketCap), np.array([sum(self.debt) * 1.1]))
        return result.x[0]

    def reverseProb(self, func=None):
        if func is not None:
            self.prob = np.array((list(map(func, self.options))))
            return

        last_step_prob = self.prob.copy()
        for i in range(self.currStep + 1):
            self.prob[i] = (last_step_prob[i + 1] * self.pr + last_step_prob[i] * (1 - self.pr)) / self.rh

    def calculateProb(self):

        index = len(self.ttm) - 1
        self.Tree(s0=self.s0, func=BinomialTree.optionExercise(self, k=self.debt[index]))
        self.prob = np.array((list(map(self.probOverride, self.options))))
        index = index - 1
        while index >= 0:
            strike_step = int(self.ttm[index] / self.ttm[-1] * self.steps)
            while self.currStep > strike_step + 1:
                self.reverse()
                self.reverseProb()
            self.reverse(func=BinomialTree.optionExercise(self, k=self.debt[index]))
            self.reverseProb(func=self.probOverride)
            index = index - 1

        while self.currStep > 0:
            self.reverse()
            self.reverseProb()
        return 1 - self.prob[0]
