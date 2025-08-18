#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
updater.py - 基于 ashare_divergence_scanner.py 重构
本版本专门用于云端数据同步，作为GitHub Actions的Updater

- 使用 akshare 作为数据源，获取更准确的A股日线历史数据
- 采用指数退避重试机制，增强下载稳定性
- 实现了高效的增量更新逻辑
- 将数据存储目标从本地SQLite改为Supabase
"""

# ───────── 0 · 基础导入 ─────────
import os
import yfinance as yf
import pandas as pd
from datetime import date, timedelta
import time
import akshare as ak
from supabase import create_client, Client
from concurrent.futures import ThreadPoolExecutor, as_completed

# ───────── 1 · 常量与配置 ─────────
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

MAX_RETRY, BASE_SLEEP = 5, 2

# ───────── 2 · 交易日历 ─────────
def latest_cn_trading_day() -> date:
    """返回 A 股市场最后已收盘交易日"""
    try:
        cal = ak.tool_trade_date_hist_sina()
        cal["trade_date"] = pd.to_datetime(cal["trade_date"]).dt.date
        return cal["trade_date"].iloc[-1]
    except Exception as e:
        print(f"❌ 获取最新交易日失败: {e}")
        # 失败时作为兜底，使用yfinance的交易日历
        td_calendar = yf.utils.get_latest_trading_day_for_calendar('XSHG')
        return pd.to_datetime(td_calendar).date()

# ───────── 3 · 数据下载与上传 ─────────
def get_latest_date_from_db(ticker: str) -> date | None:
    """从 Supabase 数据库中查询给定股票的最新日期"""
    try:
        response = supabase.table('daily_prices').select('date').eq('ticker', ticker).order('date', desc=True).limit(1).execute()
        if response.data:
            return pd.to_datetime(response.data[0]['date']).date()
    except Exception as e:
        print(f"  ❌ 查询 {ticker} 最新日期失败: {e}")
    return None

def ak_download_and_upload(ticker: str, name: str):
    """
    使用 akshare 下载日线数据并上传至 Supabase
    """
    latest_date_db = get_latest_date_from_db(ticker)

    start_date_str = "20050101" # akshare 接口要求格式
    if latest_date_db:
        # 如果数据库中存在记录，则只下载缺失的数据
        last_td = latest_cn_trading_day()
        if latest_date_db >= last_td:
            print(f"  → {ticker} 数据已是最新，跳过。")
            return
        start_date_str = (latest_date_db + timedelta(days=1)).strftime('%Y%m%d')

    print(f"✅ 处理日线数据: {ticker} ({name}) - 开始日期: {start_date_str}")
    
    for i in range(1, MAX_RETRY + 1):
        try:
            df = ak.stock_zh_a_hist(
                symbol=ticker,
                period="daily",
                start_date=start_date_str,
                adjust="qfq",
                timeout=10
            )

            if df.empty:
                print(f"  → {ticker} 数据为空，跳过。")
                return

            df['ticker'] = ticker
            df['company_name'] = name
            df["date"] = pd.to_datetime(df["日期"]).dt.date
            
            data_daily = df.rename(columns={
                "开盘": "open", "收盘": "close", "最高": "high", 
                "最低": "low", "成交量": "volume"
            })[['ticker', 'date', 'company_name', 'open', 'high', 'low', 'close', 'volume']].to_dict('records')
            
            supabase.table('daily_prices').upsert(data_daily).execute()
            print(f"  → {ticker} 日线数据上传成功，共 {len(data_daily)} 条记录。")
            return
        except Exception as e:
            if i < MAX_RETRY:
                wait_time = BASE_SLEEP * (2 ** (i - 1))
                print(f"  ⚠️ {ticker} 下载失败，正在重试... ({i}/{MAX_RETRY}) 等待 {wait_time} 秒。")
                time.sleep(wait_time)
            else:
                print(f"  ❌ {ticker} 下载/上传失败，已达最大重试次数: {e}")
                
# ───────── 4 · 主函数 ─────────
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

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(ak_download_and_upload, t, n): t for t, n in tickers_with_names}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ 股票 {ticker} 的日线数据任务执行失败: {e}")
    
    print("✅ 所有股票日线数据处理完成。")

if __name__ == "__main__":
    main()
