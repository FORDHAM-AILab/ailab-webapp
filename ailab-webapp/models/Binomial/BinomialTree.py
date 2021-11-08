# -*- coding: utf-8 -*-
"""
Created on Wed Aug  1 09:37:13 2018

@author: William Huang
"""

import math
import numpy as np
from scipy.special import comb
np.set_printoptions(suppress=True)

class binomialTree:

    def __init__(self,rf=0.05,steps=50,vol=0.2,ttm=2):
        if(steps < 1):
            raise Exception("Invalid steps", steps)
        if(vol < 0):
            raise Exception("Invalid volatility", vol)
        if(ttm < 0):
            raise Exception("Invalid time to maturity", ttm)
        self.rf = rf
        self.steps = steps
        self.vol = vol
        self.ttm = ttm
        self.h = self.ttm/self.steps
        self.rh = math.exp(self.rf*self.h)
        self.up = math.exp(self.vol*math.sqrt(self.h))
        self.dn = 1/self.up
        self.pr = (self.rh-self.dn)/(self.up-self.dn)

    
    def optionExercise(k,isCall = True):
        if(isCall):
            return lambda x: x-k if(x-k>0) else 0
        else:
            return lambda x: k-x if(k-x>0) else 0
    
    def Tree(self,s0 = 100,func = None):
        self.s0 = s0
        self.currStep = self.steps
        self.options = np.zeros(self.steps+1)
        for i in range(self.steps+1):
            self.options[i] = s0*math.pow(self.up,i)*math.pow(self.dn,self.currStep-i)
        if(func != None):
            self.options = np.array((list(map(func,self.options))))
    
    def reverse(self,Prob = False, func = None):
        lastStepOpt = self.options.copy()
        self.currStep = self.currStep - 1
        for i in range(self.currStep+1):
            self.options[i] = (lastStepOpt[i+1]*self.pr + lastStepOpt[i]*(1-self.pr))/self.rh
        if(func != None):
            self.options = np.array((list(map(func,self.options))))
            
    def reverseToBeginning(self):
        while(self.currStep > 0):
            self.reverse()
        return self.options[0]
    
    def calculateProb(self):
        self.Probs = np.zeros(self.currStep+1)
        for i in range(self.currStep+1):
            self.Probs[i] = comb(self.currStep,i)*math.pow(self.pr,i)*math.pow(1-self.pr,self.currStep-i)
        self.Probs.reshape(1,-1)
            
    def expectedResult(self, func = None):
        self.calculateProb()
        nodes = np.array((list(map(func,self.options.copy())))).reshape(-1,1)
        return np.dot(self.Probs,nodes)[0]
