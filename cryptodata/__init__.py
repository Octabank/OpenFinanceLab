import numpy as np
import pandas as pd
import json
import requests
import time
import matplotlib.pyplot as plt

MERCADO_BITCOIN = ['btcbrl', 'ltcbrl', 'ethbrl', 'xrpbrl', 'bchbrl']

class Pair:
    """Pair defines a trading pair e.g. btcusd
    
    Pair("name", start, end, period)
    Parameters:
    name
    start
    end
    period
    """
    markets = {}
    def __init__(self,name,start=pd.Timestamp(0),end=pd.Timestamp(time.time(),unit='s'),period=24*60*60):
        if start > end:
            raise ValueError("Start time should be smaller than end time.")
        self.name = str(name)
        print(name)
        self.period = period    
        self.start = start
        self.end = end
        self.markets = list_markets_containing_pair(self.name)


def rate_of_return(df):
    """calculates the rate of return for a dataframe, preserving column name"""
    cols = df.columns
    a = df[cols[1:]].values
    b = a/a[0, :]
    df2 = pd.DataFrame(b, columns = df.columns[1:])
    return df2
 

def plot_df(df):
    x = df[df.columns[0]]
    y = df[df.columns[1]]
    plt.plot(x,y)
    plt.show()
    return None
    
    
def Timestamp_from_string(s='31 12 99'):
    #print(time.mktime(time.strptime(s, "%d %m %y")))
    return pd.Timestamp(time.mktime(time.strptime(s, "%d %m %y")), unit='s')


def get_markets(api = "cryptowatch", active = True):
    """get markets available in API"""
    if api == "cryptowatch":
        try:
            r = requests.get("https://api.cryptowat.ch/markets").json()['result']
            if (active):
                exchanges = {item['exchange'] for item in r if item['active'] is True}
            else:
                exchanges = {item['exchange'] for item in r}
        except:
            print("Error: disfunctional API from Cryptowatch")
    return list(exchanges)


def get_pairs(api = "cryptowatch", active = True):
    """get pairs available in API"""
    if api == "cryptowatch":
        try:
            r = requests.get("https://api.cryptowat.ch/markets").json()['result']
            if (active):
                pairs = {item['pair'] for item in r if item['active'] is True}
            else:
                pairs = {item['pair'] for item in r}
        except:
            print("Error: disfunctional API from Cryptowatch")
    elif api == "mercado_bitcoin": 
        pairs = MERCADO_BITCOIN
    return list(pairs)


def get_ohlc(market, pair, period = 1440, api = "cryptowatch", start = "", end = "", local_timezone = True):
    """get OHLC prices available in API"""
    
    period *= 60
    period = str(period)
    params = []
    dates = ["",""]
    url = "https://api.cryptowat.ch/markets/" + market + "/" + pair + "/ohlc"
    
    for i, date in enumerate([start, end]):
        if (not date):
            dates[i] = date
        else:
            try:
                dates[i] = int(time.mktime(time.strptime(date, "%d %m %y %H %M"))) #+ local_timezone*time.timezone
            except:
                print("Error: Datetime format d m y H M")
    
    prms = {'before': dates[1], 'after': dates[0]}
    
    if api == "cryptowatch":
        try:
            r = requests.get(url, params=prms).json()['result'][period]
            a = pd.DataFrame(r, columns = ['date(local)','open','high','low','close','volume','neg'])
            a[a.columns[0]] = pd.to_datetime(a[a.columns[0]] - local_timezone*time.timezone, unit = 's')
            return a        
        except:
            print("Error: disfunctional API from Cryptowatch")
            

def get_ohlc_mercado_bitcoin(pair, start = "", end = "", local_timezone = True):
    """Only day prices"""
    #https://www.mercadobitcoin.net/api/BTC/day-summary/2013/6/20/

def get_daily_price(asset="btc", api="coinmetrics", data_type="price(usd)", start=0, end=time.time()):
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


def get_whole_market_from_coimetrics():
    assets = get_assets_from_coimetrics()
    market = {}
    print('Assets available:', assets)
    for asset in assets:
        #print('Getting data for',asset)
        market[asset] = get_daily_price(asset)
    return list(market)


def merge_market_data(market, start=Timestamp_from_string('01 01 17')):
    """merges the market data from the whole market, dumping coins which are too young"""
    assets = list(market.keys())
    #print(assets)
    #print(range(1,len(assets)))
    mkt = market[assets[0]]
    for i in range(1, len(assets)):
        if market[assets[i]]['date(utc)'][0] < start:
            mkt = pd.merge(mkt, market[assets[i]])
    
    return mkt


def get_coins_series(coins,start=Timestamp_from_string('01 01 17').timestamp()):
    returns = get_daily_price(coins[0], start=start)
    for i in range(1,len(coins)):
        returns = pd.merge(returns, get_daily_price(coins[i], start=start))
    return returns


def download_pair_ohlc_data(Pair):
    """downloads pair ohlc data into a folder"""
    try:
        os.mkdir(Pair.name)
    except:            
        shutil.rmtree(Pair.name)
        os.mkdir(Pair.name)
    for key in list(Pair.ohlc):
        Pair.ohlc[key].to_csv(Pair.name+'/'+key+'.csv', index=False)    
    return None


def load_pair_ohlc_data(name):
    """loads pair ohlc data"""
    markets = sorted(os.listdir(name))
    ohlc = dict()
    for mkt in markets:
        ohlc[mkt[0:-4]] = pd.read_csv(name+'/'+mkt)
    
    return ohlc


def cryptowatch_summary(Pair):   
    for exchange in list(Pair.markets):
        url = "https://api.cryptowat.ch/markets/"+exchange+"/"+Pair.name+"/summary"
        r = requests.get(url).json()
    return r


def cryptowatch_ohlc(pair_name, exchange, period=24*60*60,
                     start=pd.Timestamp(2018,1,1),
                     end=pd.Timestamp(time.time(), unit='s')):
    
    url = "https://api.cryptowat.ch/markets/"+exchange+"/"+pair_name+"/ohlc"
    print(url)
    try:
        r = requests.get(url, params = {'before':int(end.timestamp()), 'after': int(start.timestamp()) }).json()
        return pd.DataFrame(r['result'][str(period)],
                            columns=['CloseTime','OpenPrice','HighPrice', 'LowPrice','ClosePrice','Volume','Other'])
    except:
        print('API unavailable, error while requesting', url)
    return None
        

def list_markets_containing_pair(pair='btcusd'):
    """fetches markets on cryptowatch"""
    try:
        r = requests.get("https://api.cryptowat.ch/markets").json()['result']
        exchanges = {item['exchange'] for item in r if (item['active'] is True and item['pair'] == pair)}
        if pair in MERCADO_BITCOIN:
            exchanges.add("mercado_bitcoin")
        return exchanges
    except:
        print('error')
    return None

        
def list_pairs(pairs=get_pairs(), p1='btc'):
    """lists all cryptowatch pairs with a given coin"""
    pairs = list(pairs)
    L = []
    for pair in pairs:
        if p1 == pair[:len(p1)]:
            L.append(pair)
    return L


def list_pairs_in_market(market):
    """list all pairs traded by an exchange"""
    r = requests.get("https://api.cryptowat.ch/markets/"+market).json()['result']
    market_pairs = {item['pair'] for item in r if item['active'] is True}
    return list(market_pairs)


def cryptowatch_ohlc_data_for_pair(Pair):
    """gets ohlc data from cryptowatch, and return a dict of dataframes"""
    exchanges = list(Pair.markets)
    data = {}
    for ex in exchanges:
        data[ex] = cryptowatch_ohlc(Pair.name, ex, Pair.period, Pair.start, Pair.end)
    return list(data)


def reliable_exchanges(Pair,start=pd.Timestamp(time.time(),unit='s')-pd.Timedelta(365/2,unit='D'),
                       end=pd.Timestamp(time.time(),unit='s'), period = pd.Timedelta(1,'D')):
    """
    returns exchanges that have enough data to be on the index
    """
    try:
        Pair.ohlc
    except:
        Pair.ohlc = cryptowatch_ohlc_data_for_pair(Pair)       
    good = []
    black_list = []
    exchanges = sorted(list(Pair.markets))
    rows_needed = np.floor((end-start)/period)
    for ex in exchanges:
        if type(Pair.ohlc[ex]) == pd.DataFrame:
            ex_start = pd.Timestamp(Pair.ohlc[ex].iloc[0][0], unit='s')
            ex_end = pd.Timestamp(Pair.ohlc[ex].iloc[-1][0], unit = 's')
            if (ex_end>=end-pd.Timedelta(1,unit='D') and ex_start<start):
                good.append(ex)
    return good


def cryptowatch_volume_across_exchanges(ohlc_data, exchanges):
    data = ohlc_data[exchanges[0]][['CloseTime', 'Volume']]
    data = data.rename(index=str, columns={"CloseTime": "date(utc)", "Volume": exchanges[0]})
    for i in range(len(exchanges)):
        temp = ohlc_data[exchanges[i]][['CloseTime', 'Volume']]
        temp = temp.rename(index=str, columns={"CloseTime": "date(utc)", "Volume": exchanges[i]})
        data = pd.merge(data, temp)
    return data


def cryptowatch_closeprice_across_exchanges(ohlc_data, exchanges):
    """returns a dataframe with closeprice and date, duplicated columns are remove"""
    data = ohlc_data[exchanges[0]][['CloseTime', 'ClosePrice']]
    data = data.rename(index=str, columns={"CloseTime": "date(utc)", "ClosePrice": exchanges[0]})
    for i in range(len(exchanges)):
        temp = ohlc_data[exchanges[i]][['CloseTime', 'ClosePrice']]
        temp = temp.rename(index=str, columns={"CloseTime": "date(utc)", "ClosePrice": exchanges[i]})
        data = pd.merge(data, temp)
    return data


def pair_index(Pair,start=pd.Timestamp(time.time(),unit='s')-pd.Timedelta(365/2, unit = 'D'),
                end=pd.Timestamp(time.time(),unit='s')):
    """this function calculatex the index for a given Pair, with default 
    from the last six months, and taking into account only exchanges that were open the whole time
    """
    try:
        Pair.ohlc
    except:
        Pair.ohlc = cryptowatch_ohlc_data_for_pair(Pair)       
    ex_list = reliable_exchanges(Pair, start, end)
    a = cryptowatch_volume_across_exchanges(Pair.ohlc, ex_list)
    a = a.drop_duplicates(['date(utc)'])
    b = a[a.columns[1:]].values
    c = b.sum(axis = 1)
    d = np.reshape(np.repeat(c,b.shape[1]), b.shape)
    weights = b/d
 
    m = cryptowatch_closeprice_across_exchanges(Pair.ohlc, ex_list)
    m = m.drop_duplicates(['date(utc)'])
    n = m[m.columns[1:]].values
    o = m[m.columns[1:]]*weights
    p = o.values.sum(axis=1)

    index = pd.DataFrame({'date(utc)':m[m.columns[0]],'index':p})
    index['date(utc)'] = pd.to_datetime(index['date(utc)'], unit = 's')
    index = index[index['date(utc)'] >= start]
    return index 

if __name__ == '__main__':
    btc = Pair("btcbrl", 10, 9)
    print(btc.markets)