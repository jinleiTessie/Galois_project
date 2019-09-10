#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep  8 18:20:27 2019

@author: jinlei
"""
import dataDownloader as db
import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt
#config
start_date='20190729'
end_date='20190829'
frequency="300T"
#load data
quote=db.resampleQuote(db.loadData('quote', start_date, end_date), frequency)
trade=db.aggregateTrade(db.loadData('trade', start_date, end_date))
quote.to_csv("data/quote"+start_date+"_"+end_date+"_"+frequency+".csv", index=False)
trade.to_csv("data/trade"+start_date+"_"+end_date+".csv", index=False)
#%%
trade=pd.read_csv("data/trade"+start_date+"_"+end_date+".csv")
quote=pd.read_csv("data/quote"+start_date+"_"+end_date+"_"+frequency+".csv")
quote["imbalanced_lob"]=(quote["bidSize_balance"]-quote["askSize_balance"])/(quote["bidSize_balance"]+quote["askSize_balance"])
quote["midPrice"]=(quote["bidPrice"]+quote["askPrice"])/2
quote['midPrice_return'] = quote.sort_values('timestamp').groupby(['symbol'])["midPrice"].pct_change(1)
quote=quote.dropna()
print (quote.head())
print (len(quote))
model=sm.OLS(quote["midPrice_return"],quote["imbalanced_lob"]).fit()
print (model.summary())
quote.plot.scatter(x='imbalanced_lob',y='midPrice_return')
plt.show()
#%%
quote.to_csv("/data/processed_quote.csv")
