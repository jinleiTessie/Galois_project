#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep  8 16:27:42 2019

@author: jinlei
"""
from datetime import date, timedelta
import datetime
import pandas as pd

def loadData(tag, start, end):
    datelist=[x.strftime('%Y%m%d') for x in pd.date_range(start=start,end=end)]
    li = []
    for date in datelist:
        path=path='https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/'+tag+'/'+date+'.csv.gz'
        print ("loading data", tag, date)
        df = pd.read_csv(path,compression='gzip',error_bad_lines=False)
        li.append(df)
    frame = pd.concat(li, axis=0, ignore_index=True)
    frame["timestamp"]=pd.to_datetime(frame["timestamp"], errors='coerce',format='%Y-%m-%dD%H:%M:%S.%f')
    return frame

def resampleQuote(df, frequency):
    print ("resampling quote data to ",frequency)
    df=df.groupby('symbol').resample(frequency,on="timestamp").last()
    df=df.rename(columns={"timestamp":"order_timestamp","symbol":"symbol2"}).reset_index()
    df["timestamp_shift"]=df["timestamp"].shift(-1)
    df=df.drop(columns=['timestamp', 'symbol2']).rename(columns={"timestamp_shift":"timestamp",'bidSize':'bidSize_balance','askSize':'askSize_balance'})[['symbol','timestamp','order_timestamp','bidSize_balance', 'bidPrice', 'askPrice','askSize_balance']]
    df=df.dropna()
    return df

def aggregateTrade(df):
    df=df.groupby(['timestamp', 'symbol', 'side', 'price']).agg({'size':{'total_size': 'sum'}})
    df.columns = df.columns.droplevel(0)
    df=df.reset_index()
    return df