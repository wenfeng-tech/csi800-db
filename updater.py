import os
import akshare as ak
from supabase import create_client, Client
import pandas as pd
from datetime import date, timedelta
import yfinance as yf
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# 获取 Supabase 连接信息
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

# 初始化 Supabase 客户端
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# 雅虎财经股票代码映射函数
def get_yahoo_ticker(akshare_ticker: str) -> str:
    if akshare_ticker.startswith('6'):
        return f"{akshare_ticker}.SS"  # 上海证券交易所 (Shanghai Stock Exchange)
    elif akshare_ticker.startswith(('0', '3', '2')):
        return f"{akshare_ticker}.SZ"  # 深圳证券交易所 (Shenzhen Stock Exchange)
    return None

# 获取最新交易日
def latest_cn_trading_day() -> date:
    now = pd.Timestamp.now(tz='Asia/Shanghai')
    ref = now.date() if now.hour >= 15 else (now.date() - timedelta(1))
    cal = ak.tool_trade_date_hist_sina()
    cal["trade_date"] = pd.to_datetime(cal["trade_date"]).dt.date
    return cal.loc[cal.trade_date <= ref, "trade_date"].iloc[-1]

def process_stock(ticker: str, name: str, last_td: date):
    """
    处理单只股票的下载和上传逻辑，为并行处理设计
    """
    print(f"✅ 开始处理股票: {ticker} ({name})")
    
    # --- 获取日线数据 (带重试机制) ---
    df_daily = None
    for i in range(3):
        try:
            # 不设置start_date，akshare会从最早日期开始下载
            df_daily = ak.stock_zh_a_hist(
                symbol=ticker, 
                period="daily", 
                adjust="qfq"
            )
            if df_daily is not None and not df_daily.empty:
                df_daily["ticker"] = ticker
                df_daily["company_name"] = name
                df_daily["日期"] = pd.to_datetime(df_daily["日期"]).dt.date
                df_daily = df_daily.rename(columns={
                    "日期": "date", "开盘": "open", "收盘": "close",
                    "最高": "high", "最低": "low", "成交量": "volume"
                })
                df_daily['date'] = df_daily['date'].astype(str)
                data_daily = df_daily[['ticker', 'date', 'company_name', 'open', 'high', 'low', 'close', 'volume']].to_dict('records')
                supabase.table('daily_prices').upsert(data_daily).execute()
                print(f"  → {ticker} 日线数据上传成功，共 {len(data_daily)} 条记录。")
                break
            else:
                print(f"  → {ticker} 日线数据为空，跳过。")
                break
        except Exception as e:
            if i < 2:
                print(f"  ⚠️ {ticker} 日线数据下载失败，正在重试... ({i+1}/3)")
                time.sleep(5)
            else:
                print(f"  ❌ {ticker} 日线数据下载/上传失败: {e}")
                
    # --- 获取基本面数据 (带重试机制) ---
    data_fundamental = None
    for i in range(3):
        try:
            yahoo_ticker = get_yahoo_ticker(ticker)
            if yahoo_ticker is None:
                print(f"  → {ticker} 无法获取雅虎股票代码，跳过基本面数据。")
                break

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


def fetch_data_and_sync():
    last_td = latest_cn_trading_day()
    print(f"🚀 开始更新 A 股数据，最新交易日：{last_td}")

    try:
        cons = ak.index_stock_cons_csindex("000906")
        cons_list = cons.rename(columns={
            "成分券代码": "ticker",
            "成分券名称": "company_name"
        })[["ticker", "company_name"]].drop_duplicates()
        tickers = cons_list['ticker'].tolist()
        names = cons_list['company_name'].tolist()
    except Exception as e:
        print(f"❌ 获取中证800成分股失败: {e}")
        return
    
    # 使用 ThreadPoolExecutor 进行并行处理
    # max_workers 建议设置为 10-20，以平衡网络I/O和服务器负载
    with ThreadPoolExecutor(max_workers=20) as executor:
        # 将每个股票的任务提交到线程池
        futures = {executor.submit(process_stock, ticker, name, last_td): ticker for ticker, name in zip(tickers, names)}
        
        # as_completed 可以在任务完成后立即获取结果，而不是按提交顺序
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                future.result() # 捕捉并处理线程内部的异常
            except Exception as e:
                print(f"❌ 股票 {ticker} 的任务执行失败: {e}")
    
    print("✅ 所有股票数据处理完成。")


if __name__ == "__main__":
    fetch_data_and_sync()
