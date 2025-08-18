import os
import akshare as ak
from supabase import create_client, Client
import pandas as pd
from datetime import date, timedelta
from typing import Literal

# 获取 Supabase 连接信息
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

# 初始化 Supabase 客户端
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# 获取最新交易日
def latest_cn_trading_day() -> date:
    now = pd.Timestamp.now(tz='Asia/Shanghai')
    ref = now.date() if now.hour >= 15 else (now.date() - timedelta(1))
    cal = ak.tool_trade_date_hist_sina()
    cal["trade_date"] = pd.to_datetime(cal["trade_date"]).dt.date
    return cal.loc[cal.trade_date <= ref, "trade_date"].iloc[-1]

def fetch_data_and_sync():
    last_td = latest_cn_trading_day()
    print(f"🚀 开始更新 A 股数据，最新交易日：{last_td}")

    # 获取中证800成分股
    try:
        cons = ak.index_stock_cons_csindex("000906")
        cons_list = cons.rename(columns={
            "成分券代码": "ticker",
            "成分券名称": "company_name"
        })[["ticker", "company_name"]].drop_duplicates()
    except Exception as e:
        print(f"❌ 获取中证800成分股失败: {e}")
        return

    # 批量获取数据并上传
    for ticker, name in cons_list.itertuples(index=False):
        print(f"✅ 处理股票: {ticker} ({name})")
        
        # 获取日线数据
        try:
            df_daily = ak.stock_zh_a_hist(
                symbol=ticker, period="daily", start_date="20200101", adjust="qfq"
            )
            if df_daily is not None and not df_daily.empty:
                df_daily["ticker"] = ticker
                df_daily["company_name"] = name
                df_daily["日期"] = pd.to_datetime(df_daily["日期"]).dt.date
                df_daily = df_daily.rename(columns={
                    "日期": "date", "开盘": "open", "收盘": "close",
                    "最高": "high", "最低": "low", "成交量": "volume"
                })
                df_daily = df_daily[df_daily.date == last_td].copy()
                
                if not df_daily.empty:
                    df_daily['date'] = df_daily['date'].astype(str)
                    data_daily = df_daily[['ticker', 'date', 'company_name', 'open', 'high', 'low', 'close', 'volume']].to_dict('records')
                    supabase.table('daily_prices').upsert(data_daily).execute()
                    print(f"  → 日线数据上传成功")
                else:
                    print("  → 日线数据已最新，无需更新")
            else:
                 print("  → 日线数据为空，跳过")

        except Exception as e:
            print(f"  ❌ 日线数据下载/上传失败: {e}")
        
        # 获取基本面数据
        try:
            df_fundamental = ak.stock_financial_analysis_indicator_em(symbol=ticker)
            if df_fundamental is not None and not df_fundamental.empty:
                # 修复：确保交易日期和股息率列存在
                if '交易日期' in df_fundamental.columns and '股息率' in df_fundamental.columns:
                    df_fundamental = df_fundamental.rename(columns={
                        "交易日期": "date", 
                        "股息率": "dividend_yield"
                    })
                    df_fundamental['date'] = pd.to_datetime(df_fundamental['date']).dt.date
                    df_fundamental['ticker'] = ticker

                    df_fundamental = df_fundamental[df_fundamental.date == last_td].copy()
                    
                    if not df_fundamental.empty:
                        df_fundamental['date'] = df_fundamental['date'].astype(str)
                        data_fundamental = df_fundamental[['ticker', 'date', 'dividend_yield']].to_dict('records')
                        supabase.table('fundamental_data').upsert(data_fundamental).execute()
                        print(f"  → 基本面数据上传成功")
                    else:
                        print("  → 基本面数据已最新，无需更新")
                else:
                    print("  → 基本面数据缺失必要列，跳过")
            else:
                print("  → 基本面数据为空，跳过")

        except Exception as e:
            print(f"  ❌ 基本面数据下载/上传失败: {e}")

if __name__ == "__main__":
    fetch_data_and_sync()
