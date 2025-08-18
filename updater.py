import os
import yfinance as yf
from supabase import create_client, Client
import pandas as pd
from datetime import date, timedelta
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 全局配置和工具函数 ---

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Yahoo Finance 股票代码映射函数
def get_yahoo_ticker(akshare_ticker: str) -> str:
    if akshare_ticker.startswith('6'):
        return f"{akshare_ticker}.SS"
    elif akshare_ticker.startswith(('0', '3', '2')):
        return f"{akshare_ticker}.SZ"
    return None

# 获取最新交易日
def latest_cn_trading_day() -> date:
    today = date.today()
    td_calendar = yf.utils.get_latest_trading_day_for_calendar('XSHG')
    latest_td = pd.to_datetime(td_calendar).date()
    return latest_td

def get_latest_date_from_db(ticker: str) -> date | None:
    """
    从 Supabase 数据库中查询给定股票的最新日期
    """
    try:
        response = supabase.table('daily_prices').select('date').eq('ticker', ticker).order('date', desc=True).limit(1).execute()
        if response.data:
            return pd.to_datetime(response.data[0]['date']).date()
    except Exception as e:
        print(f"  ❌ 查询 {ticker} 最新日期失败: {e}")
    return None

# --- 核心同步函数 ---

def sync_daily_prices(tickers_with_names: list, last_td: date):
    """
    同步日线数据
    """
    print("--- 开始同步日线数据 ---")
    
    def process_daily_price(ticker: str, name: str, last_td: date):
        print(f"✅ 处理日线数据: {ticker} ({name})")
        for i in range(3):
            try:
                yahoo_ticker = get_yahoo_ticker(ticker)
                if not yahoo_ticker:
                    print(f"  → {ticker} 无法获取雅虎代码，跳过日线数据。")
                    return
                
                start_date_str = None
                latest_date_db = get_latest_date_from_db(ticker)

                if latest_date_db:
                    # 如果数据库中存在记录，则只下载缺失的数据
                    if latest_date_db >= last_td:
                        print(f"  → {ticker} 数据已是最新，跳过。")
                        return
                    start_date_str = (latest_date_db + timedelta(days=1)).strftime('%Y-%m-%d')
                
                # 下载数据：如果是新股票，则start_date为None，下载全部历史
                df = yf.Ticker(yahoo_ticker).history(
                    start=start_date_str, 
                    end=(last_td + timedelta(days=1)).strftime('%Y-%m-%d')
                )
                
                if df.empty:
                    print(f"  → {ticker} 日线数据为空或已最新，跳过。")
                    break

                df['ticker'] = ticker
                df['company_name'] = name
                df.index.name = 'date'
                df = df.reset_index()

                data_daily = df.rename(columns={
                    "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"
                })[['ticker', 'date', 'company_name', 'open', 'high', 'low', 'close', 'volume']].to_dict('records')
                
                supabase.table('daily_prices').upsert(data_daily).execute()
                print(f"  → {ticker} 日线数据上传成功，共 {len(data_daily)} 条记录。")
                break
            except Exception as e:
                if i < 2:
                    print(f"  ⚠️ {ticker} 日线数据下载失败，正在重试... ({i+1}/3)")
                    time.sleep(5)
                else:
                    print(f"  ❌ {ticker} 日线数据下载/上传失败: {e}")

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(process_daily_price, t, n, last_td): t for t, n in tickers_with_names}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ 股票 {ticker} 的日线数据任务执行失败: {e}")

def sync_fundamental_data(tickers_with_names: list, last_td: date):
    """
    同步基本面数据
    """
    print("--- 开始同步基本面数据 ---")

    def process_fundamental_data(ticker: str, name: str, last_td: date):
        print(f"✅ 处理基本面数据: {ticker} ({name})")
        for i in range(3):
            try:
                yahoo_ticker = get_yahoo_ticker(ticker)
                if not yahoo_ticker:
                    print(f"  → {ticker} 无法获取雅虎股票代码，跳过基本面数据。")
                    return
                
                stock = yf.Ticker(yahoo_ticker)
                info = stock.info
                
                if info:
                    data_fundamental = {
                        'ticker': ticker,
                        'date': last_td.strftime('%Y-%m-%d'),
                        'eps': info.get('trailingEps'),
                        'pe': info.get('trailingPE'),
                        'pb': info.get('priceToBook'),
                        'total_market_cap': info.get('marketCap'),
                        'dividend_yield': info.get('dividendYield')
                    }
                    if data_fundamental['dividend_yield'] is not None:
                        data_fundamental['dividend_yield'] *= 100

                    supabase.table('fundamental_data').upsert([data_fundamental]).execute()
                    print(f"  → {ticker} 基本面数据上传成功。")
                    break
                else:
                    print(f"  → {ticker} 基本面数据为空，跳过。")
                    break
            except Exception as e:
                if i < 2:
                    print(f"  ⚠️ {ticker} 基本面数据下载失败，正在重试... ({i+1}/3)")
                    time.sleep(5)
                else:
                    print(f"  ❌ {ticker} 基本面数据下载/上传失败: {e}")

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(process_fundamental_data, t, n, last_td): t for t, n in tickers_with_names}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ 股票 {ticker} 的基本面数据任务执行失败: {e}")

# --- 主函数 ---

def main():
    last_td = latest_cn_trading_day()
    print(f"🚀 开始更新 A 股数据，最新交易日：{last_td}")

    try:
        cons = pd.read_csv('http://www.csindex.com.cn/uploads/file/autofile/cons/000906.txt', encoding='gbk', skiprows=1, sep='\t')
        cons_list = cons.rename(columns={
            "成分券代码": "ticker",
            "成分券名称": "company_name"
        })[["ticker", "company_name"]].drop_duplicates().itertuples(index=False)
        tickers_with_names = list(cons_list)
    except Exception as e:
        print(f"❌ 获取中证800成分股失败: {e}")
        return

    sync_daily_prices(tickers_with_names, last_td)
    sync_fundamental_data(tickers_with_names, last_td)
    
    print("✅ 所有股票数据处理完成。")

if __name__ == "__main__":
    main()
