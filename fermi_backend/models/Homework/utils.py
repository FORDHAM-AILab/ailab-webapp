from models.Data.Data import FinanceData

import os.path
import pandas as pd


#BASE_DIR = 'F://Server Code//university_code'
def getPriceTableFromFileCache(tickerList, startDate, endDate):
    df = pd.DataFrame()
    dataSource = FinanceData('Yahoo')
    for ticker in tickerList:
        file_path = '/university_code/VaR/Data/{}.csv'.format(ticker) # pending
        if os.path.isfile(file_path):
            temp_df = dataSource.getPriceTable([ticker], startDate.isoformat(), endDate.isoformat(), localCheck=file_path, update=True)
        else:
            temp_df = dataSource.getPriceTable([ticker], startDate.isoformat(), endDate.isoformat())
            if(len(temp_df)):
                temp_df.to_csv(file_path, index=True)

        if df.shape[0] == 0:
            df = temp_df.copy()
        else:
            df = df.join(temp_df,how='outer')

    return df
