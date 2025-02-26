import pandas as pd, threading
import requests, datetime, yfinance, pytz
from bs4 import BeautifulSoup
from utils import load_pickle, save_pickle, ALPHA


def get_sp500_tickers():
    res = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = BeautifulSoup(res.text, 'html')
    table = soup.find_all('table')[0]
    df = pd.read_html(str(table))
    tickers = df[0]['Symbol'].tolist()
    return tickers

# tickers = get_sp500_tickers()
# print(tickers)

def get_history(ticker, start_date, end_date, granularity='1d', tries=0):
    try:
        df = yfinance.Ticker(ticker).history(start=start_date, end=end_date, interval=granularity, auto_adjust=True).reset_index()
        df = df.rename(columns={
            'Date': 'datetime', 
            'Open': 'open', 
            'High': 'high', 
            'Low': 'low', 
            'Close': 'close', 
            'Volume': 'volume'})
    except Exception as e:
        while tries < 5:
            return get_history(ticker, start_date, end_date, granularity, tries + 1)
        return pd.DataFrame()
    
    if df.empty:
        return pd.DataFrame()

    # df['datetime'] = df['datetime'] #.dt.tz_localize(pytz.utc)
    df = df.set_index('datetime', drop=True)
    df = df.drop(columns=['Dividends', 'Stock Splits'])
    # input(df)
    return df

def get_histories(tickers, start_dates, end_dates, granularity='1d'):
    dfs = [None] * len(tickers)
    def _helper(i):
        print(f'Getting history for {tickers[i]}')
        df = get_history(tickers[i], start_dates[i], end_dates[i], granularity)
        dfs[i] = df

    # for i in range(len(tickers)):
    #     _helper(i)
    threads = [threading.Thread(target=_helper, args=(i,)) for i in range(len(tickers))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    dfs = [df for df in dfs if not df.empty] 
    return tickers, dfs

# dfs = get_histories(tickers, [start_date] * len(tickers), [end_date] * len(tickers))
# print(dfs)

def get_ticker_dfs(start, end):
    try:
        tickers, dfs = load_pickle('dataset.obj')
    except Exception as e:
        print('Could not load dataset, fetching from the web ', e)
        tickers = get_sp500_tickers()
        start_dates = [start] * len(tickers)
        end_dates = [end] * len(tickers)
        tickers, dfs = get_histories(tickers, start_dates, end_dates)
        save_pickle('dataset.obj', (tickers, dfs))
    return tickers, {ticker:df for ticker, df in zip(tickers, dfs)}


start_date = datetime.datetime(2010, 1, 1, tzinfo=pytz.utc)
end_date = datetime.datetime.now(tz=pytz.utc)
tickers, ticker_dfs = get_ticker_dfs(start_date, end_date)
testfor = 20
tickers = tickers[:testfor]

alpha = ALPHA(insts=tickers, dfs=ticker_dfs, start=start_date, end=end_date)
alpha.run_simulation()


    
