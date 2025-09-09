import os
import sys
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import akshare as ak
import pandas as pd
from supabase import create_client, Client

# --- 配置区域 ---
# 从环境变量中读取 Supabase 配置，这是最佳安全实践
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")  # 此处应配置为 service_role key

# 设置并发下载的线程数，可根据运行环境的性能和网络状况调整（建议 5-15 之间）
MAX_WORKERS = 10

# --- 数据获取与处理函数 ---

def get_csi800_stock_info() -> dict:
    """
    获取最新的中证800成分股代码和对应的公司名称
    返回一个字典，格式为: {'代码': '公司名称', ...}
    """
    try:
        stock_df = ak.index_stock_cons_csindex(symbol="000906")
        print(f"成功从中证指数官网获取中证800成分股，共 {len(stock_df)} 只股票。")
        return pd.Series(stock_df['成分券名称'].values, index=stock_df['成分券代码']).to_dict()
    except Exception as e:
        print(f"错误：获取中证800成分股列表失败 - {e}")
        return {}


def get_stock_history(stock_code: str, stock_name: str, start_date: str) -> pd.DataFrame:
    """
    获取单只股票历史数据。为提高并发性能，不包含内部重试逻辑。
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


def fetch_and_insert_stocks(supabase_client: Client, stock_info: dict, start_date: str):
    """
    【通用模式】使用并发技术获取股票数据，并批量插入数据库。
    用于 'daily', 'full', 'partial' 模式。
    """
    all_data_frames = []
    
    # 使用线程池进行并发下载
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
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
                    # 某些股票在指定日期范围内可能无数据，属于正常情况
                    pass
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
        supabase_client.table("csi800_daily_data").upsert(data_to_insert).execute()
        print(f"🎉 成功批量同步 {len(all_data_frames)} 只股票，共 {len(data_to_insert)} 条记录。")
    except Exception as e:
        print(f"数据库错误：批量插入数据失败 - {e}")


def verify_and_retry_sync(supabase_client: Client):
    """【校验修复模式 v2.0 - 精确修复】
    检查数据完整性，并为每只落后股票计算精确的起始日期进行同步。
    """
    print("开始执行数据校验与修复 (精确模式)...")
    
    target_stocks = get_csi800_stock_info()
    if not target_stocks:
        print("无法获取目标股票列表，校验任务终止。")
        return
    
    try:
        response = supabase_client.table("csi800_daily_data").select("stock_code, trade_date").execute()
        db_data = pd.DataFrame(response.data)
        db_summary = {}
        if not db_data.empty:
            db_summary = db_data.groupby('stock_code')['trade_date'].max().to_dict()
    except Exception as e:
        print(f"从Supabase获取数据状态失败: {e}。无法执行精确修复。")
        return

    retry_tasks = []
    latest_trading_day_threshold = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
    fallback_start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

    for code, name in target_stocks.items():
        if code in db_summary:
            last_date_str = db_summary[code]
            if last_date_str < latest_trading_day_threshold:
                # 核心优化点: 计算缺失的起始日期 (最后成功日期的后一天)
                start_date_obj = datetime.strptime(last_date_str, '%Y-%m-%d') + timedelta(days=1)
                start_date_for_fetch = start_date_obj.strftime('%Y%m%d')
                retry_tasks.append((code, name, start_date_for_fetch))
                print(f"发现落后股票: {code} ({name}), 最新日期: {last_date_str}。将从 {start_date_for_fetch} 开始同步。")
        else:
            # 股票完全缺失，使用默认回溯期
            retry_tasks.append((code, name, fallback_start_date))
            print(f"发现缺失股票: {code} ({name})。将从 {fallback_start_date} 开始同步。")

    if not retry_tasks:
        print("✅ 数据校验完成，所有股票数据都是最新的。")
        return

    print(f"\n共发现 {len(retry_tasks)} 个修复任务。开始执行...")
    
    all_repaired_data = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(get_stock_history, code, name, start_date): (code, name) 
                   for code, name, start_date in retry_tasks}

        for future in as_completed(futures):
            code, name = futures[future]
            try:
                df = future.result()
                if not df.empty:
                    all_repaired_data.append(df)
                    print(f"成功修复 {code} ({name}) 的 {len(df)} 条数据。")
            except Exception as e:
                print(f"修复股票 {code} ({name}) 时发生错误: {e}")

    if not all_repaired_data:
        print("未获取到任何修复数据，无需插入。")
        return

    full_df = pd.concat(all_repaired_data, ignore_index=True)
    data_to_insert = full_df.to_dict(orient='records')
    
    print(f"\n准备批量插入 {len(data_to_insert)} 条修复数据...")
    try:
        supabase_client.table("csi800_daily_data").upsert(data_to_insert).execute()
        print(f"🎉 成功批量修复 {len(all_repaired_data)} 只股票，共插入 {len(data_to_insert)} 条新记录。")
    except Exception as e:
        print(f"数据库错误：批量插入修复数据失败 - {e}")

    print("🚀 校验与修复任务完成！")


def main():
    """脚本主入口，根据命令行参数选择运行模式"""
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
