import os
import time
import akshare as ak
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 配置区域 ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
# 设置并发下载的线程数，可根据运行环境的性能和网络状况调整（建议 5-15 之间）
MAX_WORKERS = 10 

# --- 数据获取逻辑 (基本不变，但去除了内部重试) ---
def get_stock_history(stock_code: str, stock_name: str, start_date: str) -> pd.DataFrame:
    """
    获取单只股票历史数据。为提高并发性能，取消了函数内的重试。
    由调用方决定是否以及如何重试。
    """
    try:
        stock_hist_df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, adjust="qfq")

        if not stock_hist_df.empty:
            stock_hist_df.rename(columns={
                '日期': 'trade_date', '开盘': 'open', '收盘': 'close', '最高': 'high',
                '最低': 'low', '成交量': 'volume'
            }, inplace=True)
            stock_hist_df['stock_code'] = stock_code
            stock_hist_df['stock_name'] = stock_name
            stock_hist_df['trade_date'] = pd.to_datetime(stock_hist_df['trade_date']).dt.strftime('%Y-%m-%d')
            
            required_columns = [
                'trade_date', 'stock_code', 'stock_name',
                'open', 'high', 'low', 'close', 'volume'
            ]
            return stock_hist_df[required_columns]
    except Exception as e:
        print(f"警告：获取股票 {stock_code} ({stock_name}) 数据失败 - {e}")
    return pd.DataFrame()

# --- 核心同步函数 (并发+批量插入) ---
def fetch_and_insert_stocks(supabase_client: Client, stock_info: dict, start_date: str):
    """
    使用并发技术获取股票数据，并批量插入数据库。
    Args:
        supabase_client: Supabase客户端实例。
        stock_info: 包含 {代码: 名称} 的字典。
        start_date: 数据开始日期。
    """
    all_data_frames = []
    
    # 使用线程池进行并发下载
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 创建未来任务列表
        futures = {executor.submit(get_stock_history, code, name, start_date): (code, name) 
                   for code, name in stock_info.items()}

        total_stocks = len(stock_info)
        for i, future in enumerate(as_completed(futures)):
            code, name = futures[future]
            try:
                df = future.result()
                if not df.empty:
                    all_data_frames.append(df)
                    print(f"进度: {i + 1}/{total_stocks} | 成功获取 {code} ({name}) 的 {len(df)} 条数据。")
                else:
                    print(f"进度: {i + 1}/{total_stocks} | 股票 {code} ({name}) 未返回数据。")
            except Exception as e:
                print(f"进度: {i + 1}/{total_stocks} | 处理股票 {code} ({name}) 时发生严重错误: {e}")

    if not all_data_frames:
        print("未获取到任何股票数据，无需插入。")
        return

    # 合并所有DataFrame并进行批量插入
    full_df = pd.concat(all_data_frames, ignore_index=True)
    data_to_insert = full_df.to_dict(orient='records')
    
    print(f"\n准备批量插入 {len(data_to_insert)} 条数据...")
    try:
        # Supabase 的 upsert 对大批量数据可能有限制，可以考虑分批
        # 但对于几万到几十万行通常是没问题的
        supabase_client.table("csi800_daily_data").upsert(data_to_insert).execute()
        print(f"🎉 成功批量同步 {len(all_data_frames)} 只股票，共 {len(data_to_insert)} 条记录。")
    except Exception as e:
        print(f"数据库错误：批量插入数据失败 - {e}")

# --- 新增：校验和修复逻辑 ---
def verify_and_retry_sync(supabase_client: Client):
    """【校验修复模式】检查数据完整性，并只同步缺失或落后的股票。"""
    print("开始执行数据校验与修复...")
    
    # 1. 获取目标股票列表 (中证800)
    target_stocks = get_csi800_stock_info()
    if not target_stocks:
        print("无法获取目标股票列表，校验任务终止。")
        return
    
    target_stock_codes = set(target_stocks.keys())
    print(f"目标同步 {len(target_stock_codes)} 只中证800成分股。")

    # 2. 从数据库获取当前数据状态
    try:
        # 使用rpc调用您提供的SQL函数，假设您已在Supabase后台创建了它
        # 如果没有创建SQL函数，也可以直接在python端进行数据处理
        response = supabase_client.table("csi800_daily_data").select("stock_code, trade_date").execute()
        db_data = pd.DataFrame(response.data)
        if db_data.empty:
             print("数据库中无数据，将同步所有股票。")
             stocks_to_retry = target_stocks
        else:
            db_summary = db_data.groupby('stock_code')['trade_date'].max().to_dict()
            print(f"数据库中已有 {len(db_summary)} 只股票的数据。")

            # 3. 找出需要重试的股票
            # 条件1：数据库里根本没有的股票
            # 条件2：数据库里最新日期早于N天前的股票（例如3天前）
            stocks_to_retry = {}
            latest_trading_day_threshold = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')

            for code, name in target_stocks.items():
                if code not in db_summary:
                    stocks_to_retry[code] = name # 缺失的股票
                elif db_summary[code] < latest_trading_day_threshold:
                    stocks_to_retry[code] = name # 数据落后的股票
            
    except Exception as e:
        print(f"从Supabase获取数据状态失败: {e}。将尝试同步所有股票作为备用方案。")
        stocks_to_retry = target_stocks

    if not stocks_to_retry:
        print("✅ 数据校验完成，所有股票数据都是最新的。")
        return

    print(f"发现 {len(stocks_to_retry)} 只股票需要同步/修复。列表: {list(stocks_to_retry.keys())}")
    
    # 使用与主任务相同的并发逻辑，但只处理需要重试的股票
    # 对于修复任务，我们通常希望获取最近一段时间的数据，比如最近一年
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
    fetch_and_insert_stocks(supabase_client, stocks_to_retry, start_date)
    print("🚀 校验与修复任务完成！")


def get_csi800_stock_info() -> dict:
    # (此函数与您原来的一样，无需改动)
    try:
        stock_df = ak.index_stock_cons_csindex(symbol="000906")
        print(f"成功从中证指数官网获取中证800成分股，共 {len(stock_df)} 只股票。")
        return pd.Series(stock_df['成分券名称'].values, index=stock_df['成分券代码']).to_dict()
    except Exception as e:
        print(f"错误：获取中证800成分股列表失败 - {e}")
        return {}

# --- 主逻辑入口 ---
def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("关键配置缺失：请设置 SUPABASE_URL 和 SUPABASE_KEY 环境变量。")

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    sync_mode = sys.argv[1] if len(sys.argv) > 1 else 'daily'
    print(f"--- 当前运行模式: {sync_mode} ---")

    if sync_mode == 'daily':
        print("执行每日增量更新...")
        stock_info = get_csi800_stock_info()
        if stock_info:
            start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
            fetch_and_insert_stocks(supabase, stock_info, start_date)
    
    elif sync_mode == 'verify':
        verify_and_retry_sync(supabase)

    elif sync_mode in ['full', 'partial']:
        start_dates = {'full': "20050101", 'partial': "20150101"}
        print(f"执行历史数据同步 ({sync_mode})...")
        stock_info = get_csi800_stock_info()
        if stock_info:
            fetch_and_insert_stocks(supabase, stock_info, start_dates[sync_mode])

    else:
        print(f"错误：无法识别的模式 '{sync_mode}'。请选择 'full', 'partial', 'daily' 或 'verify'。")

if __name__ == "__main__":
    main()
