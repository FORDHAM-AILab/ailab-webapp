import numpy as np
import pandas as pd
import talib


class Analysis:
    def __init__(self,df:pd.DataFrame): # series index is 'date', columns are supposed to be 'Open','High','Low','Close','Adj Close','Volume' like yahoo finance download
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Data must be a pandas dataframe.")
        self.df = df

        date_cols = [col for col in df.columns if 'date' in col.lower()]
        if len(date_cols) > 0:
            self.df.set_index(date_cols)
            self.df.drop(date_cols, axis=1, inplace=True)

        self.close = df['Adj Close']
        self.high = df['High']
        self.low = df['Low']
        self.volume = df['Volume']

    # Technical signals:
    def signals(self):
        index = ['Momentum','RSI_6','RSI_24','MA_5','MA_30','BOL_up','BOL_mid','BOL_low',
                 'ADX','ADXR','APO','AROON','AROONOSC','BOP','CCI','CMO','DX','MACD','MACDEXT',
                 'MACDFIX','MFI','MINUS_DI','MINUS_DM','PLUS_DI','PLUS_DM','PPO','ROC','ROCR',
                 'STOCH_k','STOCH_d','TRIX','ULTOSC','WILLR']
        close = self.close
        high = self.high
        low  = self.low
        volume = self.volume
        signal = pd.Series(0,index=index)

        signal['Momentum'] = talib.MOM(close, timeperiod=35)[-1] # Get the up-to-date 35-day momentum
        signal['RSI_6'] = talib.RSI(close, timeperiod=6)[-1] # Get the up-to-date 6-day RSI
        signal['RSI_24'] = talib.RSI(close, timeperiod=24)[-1]  # Get the up-to-date 24-day RSI
        signal['MA_5'] = talib.MA(close, timeperiod=5)[-1]  # Get the up-to-date 5-day moving average
        signal['MA_30'] = talib.MA(close, timeperiod=30)[-1]  # Get the up-to-date 30-day moving average
        signal['BOL_up'],signal['BOL_mid'],signal['BOL_low'] = talib.BBANDS(close, nbdevup=1, nbdevdn=1, timeperiod=5)[-1]  # Get the up-to-date bollinger bands

        signal['ADX'] = talib.ADX(high, low, close, timeperiod=14)[-1]
        signal['ADXR'] = talib.ADXR(high, low, close, timeperiod=14)[-1]
        signal['APO'] = talib.APO(close, fastperiod=12, slowperiod=26, matype=0)[-1]
        signal['AROON'] = talib.AROON(high, low, timeperiod=14)[-1]
        signal['AROONOSC'] = talib.AROONOSC(high, low, timeperiod=14)[-1]
        signal['BOP'] = talib.BOP(open, high, low, close)[-1]
        signal['CCI'] = talib.CCI(high, low, close, timeperiod=14)[-1]
        signal['CMO'] = talib.CMO(close, timeperiod=14)[-1]
        signal['DX'] = talib.DX(high, low, close, timeperiod=14)[-1]
        macd, macdsignal, macdhist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)[-1]
        signal['MACD'] = macd
        macd, macdsignal, macdhist = talib.MACDEXT(close, fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0,
                                             signalperiod=9, signalmatype=0)[-1]
        signal['MACDEXT'] = macd
        macd, macdsignal, macdhist = talib.MACDFIX(close, signalperiod=9)[-1]
        signal['MACDFIX'] = macd
        signal['MFI'] = talib.MFI(high, low, close, volume, timeperiod=14)[-1]
        signal['MINUS_DI'] = talib.MINUS_DI(high, low, close, timeperiod=14)[-1]
        signal['MINUS_DM'] = talib.MINUS_DM(high, low, timeperiod=14)[-1]
        signal['PLUS_DI'] = talib.PLUS_DI(high, low, close, timeperiod=14)[-1]
        signal['PLUS_DM'] = talib.PLUS_DM(high, low, timeperiod=14)[-1]
        signal['PPO'] = talib.PPO(close, fastperiod=12, slowperiod=26, matype=0)[-1]
        signal['ROC'] = talib.ROC(close, timeperiod=10)[-1]
        signal['ROCR'] = talib.ROCR(close, timeperiod=10)[-1]
        slowk, slowd = talib.STOCH(high, low, close, fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3,
                             slowd_matype=0)[-1]
        signal['STOCH_k'] = slowk
        signal['STOCH_d'] = slowd
        signal['TRIX'] = talib.TRIX(close, timeperiod=30)[-1]
        signal['ULTOSC'] = talib.ULTOSC(high, low, close, timeperiod1=7, timeperiod2=14, timeperiod3=28)[-1]
        signal['WILLR'] = talib.WILLR(high, low, close, timeperiod=14)[-1]

        return signal