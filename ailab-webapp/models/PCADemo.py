from VaR import PCAVaR
import pandas as pd
import numpy as np
from Data import FinanceData


tickerList = pd.read_csv('VaR/Data/universeTickerList.csv',header = None).values.reshape(-1)

DataSource = FinanceData()
startDate = '2017-10-2'
endDate = '2018-8-31'

universe = DataSource.getPriceTable(tickerList,startDate,endDate,localCheck = 'VaR/Data/universe.csv',update = True)
'Local data from 2017-10-01 to 2018-10-08'
