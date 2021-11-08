# -*- coding: utf-8 -*-
"""
Created on Thu Sep  6 14:51:13 2018

@author: William Huang
"""


from VaR import ValueAtRisk
from VaR import HistoricalVaR
from Liquidity import Liquidity
from Geske import Geske
from Data import FinanceData

import numpy as np
import math
import datetime as dt
from bdateutil import isbday
from bdateutil import relativedelta
import holidays
import pyodbc
import logging


class Homework:
	def __init__(self,approach='Yahoo'):
		self.dataSource = FinanceData(approach)

	# start date 252, 0.01 default value
	# check whether endDate is business date, if not move backward
	def Homework1(self,tickerList,weight,notional,startDate = None,endDate = None,historicalWindow = 252,intervals = [0.995,0.99,0.98,0.975,0.95]):
		if(startDate == None and endDate == None):
			today = dt.date.today()
			today = today - dt.timedelta(days =1)
			# Get the latest business day
			while(isbday(today,holidays = holidays.US()) == False):
				today = today - dt.timedelta(days =1)


			endDate = today
			startDate = today + relativedelta(bdays=-historicalWindow)


		result = {}
		paraDict = {}
		hisDict = {}
		weight = np.array(weight)
		priceTabel = self.dataSource.getPriceTable(tickerList,startDate.isoformat(),endDate.isoformat())
		while(len(priceTabel) < historicalWindow):
			startDate = startDate - dt.timedelta(historicalWindow - len(priceTabel))
			priceTabel = self.dataSource.getPriceTable(tickerList, startDate.isoformat(), endDate.isoformat())
		parametric = ValueAtRisk(0.95,priceTabel.as_matrix(),weight)
		historical = HistoricalVaR(0.95,priceTabel.as_matrix(),weight)
		for interval in intervals:
			parametric.setCI(interval)
			historical.setCI(interval)
			key = interval
			# the 1 here means daily var
			paraDict[key] = parametric.var(marketValue=notional,window = 1)
			hisDict[key] = historical.var(marketValue=notional,window = historicalWindow)
		result['Parametric'] = paraDict
		result['Historical'] = hisDict
		result['Cov-Var Matrix'] = parametric.covMatrix()
		return result


	# the unit for debt and marketCap is Billion
	def Homework2(self,ticker, ttm, Debt, rf = 0.05, steps = 200):
		vol = self.dataSource.getVol(ticker)
		marketCap = self.dataSource.getMarketCap(ticker)
		marketCap = marketCap/1000000000
		GDemo = Geske(rf=rf,steps=steps,vol=vol,ttm=ttm[-1])
		result = {}
		result['Asset Price'] = GDemo.getAssetPrice(ttm,Debt,marketCap)
		result['Default Prob'] = GDemo.calculateProb(s0=result['Asset Price'],ttm = ttm,Debt = Debt)
		result['Market Cap'] = marketCap
		return result


	# rf default 0.05

	def Homework3(self,ticker,t1,t2,k1,k2,rf = 0.05,div = 0,sharpRatio = 1.6, steps = 200):
		result = {}
		vol = self.dataSource.getVol(ticker)
		marketCap = self.dataSource.getMarketCap(ticker)/1000000000
		logging.debug(f'{ticker} - MarketCap: {marketCap} Billion')
		ttm = [t1,t2]
		Debt = [k1,k2]
		LiquidA0 = self.Homework2(ticker,ttm,Debt,steps = steps)['Asset Price']
		logging.debug(f'{ticker} - Liquid Asset Value: {LiquidA0} Billion')
		temp = math.log((k1+k2)/(LiquidA0-marketCap))
		adjustFactor = 1-(0.1-temp)*4
		logging.debug(f'{ticker} - Adjust Factor: {adjustFactor}')
		try:
			W = Liquidity.calibrateWealth(adjustFactor,0,0,0.3,1,1,LiquidA0)
			logging.debug(f'{ticker} - Wealth: {W}')
			illquidA0 = Liquidity.illquidPrice(W,W*adjustFactor,t1,rf,sharpRatio,0.3)
			logging.debug(f'{ticker} - Illiquid Asset Value: {illquidA0}')
			LDemo = Liquidity(k = 1, rf = rf, steps = steps, vol = vol, ttm = t1)
			LDemo.liquidPrice(s0 = illquidA0,k1= k1,k2 = k2,t1 = t1, t2 = t2,div = div)
			GDemo = Geske(rf=rf,steps=steps,vol=vol,ttm=t2)
			illquidE0 = GDemo.geske(illquidA0,ttm,Debt)
		except:
			return -1
		result['Illiquid Asset'] = illquidA0
		result['Liquid Asset'] = LiquidA0
		result['Illiquid Equity'] = illquidE0
		result['Market Cap'] = marketCap
		return result
		


'''
tickerList = ['AMZN', 'MSFT', 'GOOG']
weight = [0.5, 0.3, 0.2]
startDate = '2017-8-10'
endDate = '2018-9-11'
notional = 100000
historicalWindow = 100

demo = Homework()
# print(demo.Homework1(tickerList,weight,notional,startDate,endDate))
print('Homework1',demo.Homework1(tickerList,weight,notional))

ticker = 'AMZN'
rf = 0.05
ttm = [1,2]
Debt = [6000,20]
# marketCap = 80
print('\nHomework2',demo.Homework2(ticker,ttm,Debt))

k1 = 900
t1 = 1

k2 = 1100
t2 = 2

#obeservable equity price
marketCap = 21713.43

#rf & volatility
rf = 0.0
vol = 0.210609
div = 0
pcFlag = 1
ttm = [t1,t2]
Debt = [k1,k2]
sharpRatio = 0.23
print('\nHomework3',demo.Homework3(ticker,t1,t2,k1,k2,rf,div,sharpRatio,steps = 200))
'''