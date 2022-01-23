import pandas as pd


df = pd.read_csv('/Users/xuanmingcui/Documents/projects/ailab-webapp/ailab-webapp/models/VaR/Data/portfolio.csv')

cov = df.cov()