# -*- coding: utf-8 -*-
"""
Created on Wed Aug  1 11:05:32 2018

@author: kaihu
"""

from Binomial import binomialTree
import numpy as np
from scipy.optimize import root

class Geske(binomialTree):
	'ttm and Debt are array'
	def geske(self,s0,ttm,Debt):
		index = len(ttm)-1
		self.Tree(s0=s0,func = binomialTree.optionExercise(k=Debt[index]))

		index = index - 1
		while(index >= 0):
			strikeStep = int(ttm[index]/ttm[-1]*self.steps)
			while(self.currStep>strikeStep+1):
				self.reverse()

			self.reverse(func = binomialTree.optionExercise(k=Debt[index]))

			index = index - 1

		return self.reverseToBeginning()

	def probOverride(self,optionValue):
		if(optionValue > 0):
			return 1.0
		else:
			return 0.0

	def errorFunc(self,ttm,Debt,marketCap):
		return lambda x: self.geske(s0=x,ttm = ttm,Debt = Debt)-marketCap

	def getAssetPrice(self,ttm,Debt,marketCap):
		result = root(self.errorFunc(ttm=ttm,Debt = Debt, marketCap = marketCap),[sum(Debt)*1.1])
		return result.x[0]


	def reverseProb(self,func = None):
		if (func != None):
			self.Prob = np.array((list(map(func, self.options))))
			return

		lastStepProb = self.Prob.copy()
		for i in range(self.currStep+1):
			self.Prob[i] = (lastStepProb[i+1]*self.pr + lastStepProb[i]*(1-self.pr))/self.rh


	def calculateProb(self,s0,ttm,Debt):

		index = len(ttm)-1
		self.Tree(s0=s0,func = binomialTree.optionExercise(k=Debt[index]))

		self.Prob = np.zeros(self.steps+1)
		self.Prob = np.array((list(map(self.probOverride,self.options))))
		index = index - 1
		while(index >= 0):
			strikeStep = int(ttm[index]/ttm[-1]*self.steps)
			while(self.currStep>strikeStep+1):
				self.reverse()
				self.reverseProb()
			self.reverse(func = binomialTree.optionExercise(k=Debt[index]))
			self.reverseProb(func = self.probOverride)
			index = index - 1

		while(self.currStep > 0):
			self.reverse()
			self.reverseProb()
		return 1 - self.Prob[0]

