import numpy as np
import pandas as pd
import json
import requests
import time
from cryptodata import get_markets, get_pairs, get_ohlc
import os
import shutil
import matplotlib.pyplot as plt


def coinmetrics_daily_price(asset="btc", api="coinmetrics", data_type="price(usd)", start=0, end=time.time()):
    if (api == "coinmetrics"):
        try:
            url =  "https://coinmetrics.io/api/v1/get_asset_data_for_time_range/"+asset+"/"+data_type
            url = url + "/"+str(int(start)) +"/"+str(int(end))
            r = requests.get(url).json()['result']
            a = pd.DataFrame(r, columns = ['date(utc)',str(asset)+" "+str(data_type)])
            #print(url)
            a[a.columns[0]] = pd.to_datetime(a[a.columns[0]], unit = 's')
            return a
        except:
            print("Error: disfunctional API from Coinmetrics")                        
    else:
        return 0


def get_assets_from_coimetrics():
    url = 'https://coinmetrics.io/api/v1/get_supported_assets'
    r  = requests.get(url).json()
    return r


def coimetrics_get_whole_market(data_type='price(usd)'):
    """downloads the data_type data from coinmetrics, for all assets"""
    assets = get_assets_from_coimetrics()
    market = {}
    #print('Assets available:', assets)
    for asset in assets:
        #print('Getting data for',asset)
        market[asset] = coinmetrics_daily_price(asset=asset,data_type=data_type)
    return market


def merge_market_data(market, start=pd.Timestamp(2018,1,1)):
    '''merges the market data from the whole market, dumping coins which are too young '''
    assets = list(market.keys())
    #print(assets)
    #print(range(1,len(assets)))
    mkt = market[assets[0]]
    for i in range(1, len(assets)):
        if market[assets[i]]['date(utc)'][0] < start:
            mkt = pd.merge(mkt, market[assets[i]])    
    return mkt


def get_coins_series(coins,start=pd.Timestamp(2018,1,1).timestamp()):
    returns = get_daily_price(coins[0], start=start)
    for i in range(1,len(coins)):
        returns = pd.merge(returns, get_daily_price(coins[i], start=start))
    return returns

 

def rate_of_return(df):
    """calculates the rate of return for a dataframe, but as is also removes time data"""
    cols = df.columns
    #print(cols)
    if (type(df[cols[0]][0]) == pd.Timestamp):
        a = df[cols[1:]].values
        b = a/a[0, :]
        df2 = pd.DataFrame(b, columns = df.columns[1:])
        #df2 = pd.merge(df2, pd.Dataframe(b, columns = cols[1:]))
        #print(b)
        
        return df2
    else:
        print('error at return_from_values function')


def get_portfolio_data_from_market_dataframe(mkt, coins):
    cols = [mkt.columns[0]]
    for coin in coins:
        cols.append(coin+' price(usd)')
        
    return mkt[cols]


def markovitz_monte_carlo(portfolio, plot_data=True):
    """
    this function simulates the performance of random portfolios, 
    and finds the best one by sharpe ratio and by least risk
    """
    portfolio_returns = rate_of_return(portfolio)
    num_samples = 10_000
    s = np.zeros(num_samples)
    m = np.zeros(num_samples)
    mu = portfolio_returns.mean()
    sigma = portfolio_returns.cov()
    sim_data = {}
    for i in range(num_samples):
        w1 = np.random.rand(mu.shape[0])
        w2 = np.sum(w1)
        w = w1/w2
        m[i] = np.matmul(mu, w)
        s[i] = np.sqrt( np.matmul(np.matmul(w, sigma), w) )
        #we filter data with high risk:
        if s[i] < 2:
            sim_data[tuple(w)] = (m[i], s[i], m[i]/s[i])
    sharpe = np.iinfo(np.int32).min
    min_risk = np.iinfo(np.int32).max
    s_plot = []
    m_plot = []
    for w in list(sim_data):
        m_plot.append(sim_data[w][0])
        s_plot.append(sim_data[w][1])        
        temp_sharpe = sim_data[w][2]
        if temp_sharpe > sharpe:
            wsharpe = w
            sharpe = temp_sharpe
        temp_risk = sim_data[w][1]
        if temp_risk < min_risk :
            min_risk = temp_risk
            wmin = w

    if plot_data == True:
        plt.scatter(s_plot,m_plot,1,'b')
        plt.scatter(sim_data[wsharpe][1], sim_data[wsharpe][0], 60,'r')
        plt.scatter(sim_data[wmin][1], sim_data[wmin][0], 30,'k')
        plt.show()
    
    return sim_data, wsharpe, wmin, sharpe, min_risk


def worst_returns(mkt, num = 5):
    """given a rate_of_return Dataframe, returns the worst ones"""
    a = np.array(mkt.iloc[-1])
    b = np.argsort(a)
    worst = []
    for t in b:
        if t < num:
            worst.append(mkt.columns[b[t]])
        
    return mkt[worst]


def best_returns(mkt, num = 5):
    """given a rate_of_return Dataframe, returns the best ones"""
    a = np.array(mkt.iloc[-1])
    b = np.argsort(a)
    best = []
    for t in b:
        if t > b.shape[0] -1 - num:
            best.append(mkt.columns[b[t]])
        
    return mkt[best]