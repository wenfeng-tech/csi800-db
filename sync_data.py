import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import akshare as ak
import pandas as pd
from supabase import create_client, Client

# --- 配置区域 ---

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

# 从环境变量中读取 Supabase 配置
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# 设置并发下载的线程数
MAX_WORKERS = 10
# 设置数据库分批插入的大小
BATCH_SIZE = 50

# --- 数据获取与处理函数 ---

def get_csi800_stock_info() -> dict:
    """
    获取最新的中证800成分股代码和对应的公司名称
    """
    try:
        stock_df = ak.index_stock_cons_csindex(symbol="000906")
        logging.info(f"成功从中证指数官网获取中证800成分股，共 {len(stock_df)} 只股票。")
        return pd.Series(stock_df['成分券名称'].values, index=stock_df['成分券代码']).to_dict()
    except Exception as e:
        logging.error(f"错误：获取中证800成分股列表失败 - {e}")
        return {}


def get_stock_history(stock_code: str, stock_name: str, start_date: str) -> pd.DataFrame:
    """
    获取单只股票历史数据。不包含内部重试逻辑。
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
        logging.warning(f"获取股票 {stock_code} ({stock_name}) 数据失败 - {e}")
        raise  # 重新抛出异常，以便上层知道失败了

    return pd.DataFrame()


def execute_batch_upsert(supabase_client: Client, data_frames: list) -> int:
    """辅助函数：执行批量插入并返回插入的记录数"""
    if not data_frames:
        return 0
        
    full_df = pd.concat(data_frames, ignore_index=True)
    data_to_insert = full_df.to_dict(orient='records')
    record_count = len(data_to_insert)
    
    logging.info(f"准备批量插入 {record_count} 条数据 (批次)...")
    try:
        supabase_client.table("csi800_daily_data").upsert(data_to_insert).execute()
        logging.info(f"✅ 成功同步批次，共 {record_count} 条记录。")
        return record_count
    except Exception as e:
        logging.error(f"数据库错误：批次插入数据失败 - {e}")
        return 0


def fetch_and_insert_stocks(supabase_client: Client, stock_info: dict, start_date: str, task_desc: str):
    """
    【通用模式】使用并发技术获取股票数据，并分批插入数据库。
    """
    logging.info(f"开始执行 '{task_desc}' 任务，目标股票数: {len(stock_info)}，起始日期: {start_date}")
    
    batch_data_frames = []
    total_inserted_records = 0
    total_stocks = len(stock_info)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(get_stock_history, code, name, start_date): (code, name) 
                   for code, name in stock_info.items()}

        for i, future in enumerate(as_completed(futures)):
            code, name = futures[future]
            try:
                df = future.result()
                if not df.empty:
                    batch_data_frames.append(df)
                    logging.info(f"进度: {i + 1}/{total_stocks} | 成功获取 {code} ({name}) 的 {len(df)} 条数据。")
            except Exception as e:
                logging.error(f"进度: {i + 1}/{total_stocks} | 处理股票 {code} ({name}) 时发生严重错误: {e}")

            # 分批处理逻辑
            if len(batch_data_frames) >= BATCH_SIZE or (i + 1) == total_stocks:
                inserted_count = execute_batch_upsert(supabase_client, batch_data_frames)
                total_inserted_records += inserted_count
                batch_data_frames = [] # 清空批次

    logging.info(f"🎉 '{task_desc}' 任务完成！总共成功插入 {total_inserted_records} 条记录。")


def verify_and_retry_sync(supabase_client: Client, target_stocks: dict):
    """
    【校验修复模式 - 简化版】
    检查数据库，找出所有未达到最新交易日期的股票，并对它们统一执行一次小规模的每日更新。
    """
    logging.info("开始执行数据校验与修复 (简化模式)...")
    
    if not target_stocks:
        logging.warning("目标股票列表为空，校验任务终止。")
        return
    
    try:
        # 1. 获取数据库中所有记录，以确定基准日期
        response = supabase_client.table("csi800_daily_data").select("stock_code, trade_date").execute()
        db_data = pd.DataFrame(response.data)
        
        if db_data.empty:
            logging.warning("数据库为空，无法执行校验。建议先运行 'partial' 或 'full' 模式进行初始化。")
            # 将所有目标股票视为缺失，并进行一次每日更新
            logging.info("将为所有目标股票执行一次每日增量同步...")
            start_date_for_retry = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
            fetch_and_insert_stocks(supabase_client, target_stocks, start_date_for_retry, "数据库初始化修复")
            return
            
        # 2. 确定唯一的最新交易日作为基准
        latest_market_date = db_data['trade_date'].max()
        logging.info(f"数据库中的最新交易日基准为: {latest_market_date}")

        # 3. 找出所有落后或缺失的股票
        db_summary = db_data.groupby('stock_code')['trade_date'].max()
        
        retry_stock_info = {}
        # 找出数据落后的股票
        lagging_codes = db_summary[db_summary < latest_market_date].index
        for code in lagging_codes:
            if code in target_stocks:
                retry_stock_info[code] = target_stocks[code]

        # 找出完全缺失的股票
        db_codes = set(db_summary.index)
        for code, name in target_stocks.items():
            if code not in db_codes:
                retry_stock_info[code] = name
        
        if not retry_stock_info:
            logging.info(f"✅ 数据校验完成，所有股票数据都已更新至 {latest_market_date}。")
            return

        logging.info(f"\n共发现 {len(retry_stock_info)} 只股票未达到最新日期。准备进行一次针对性的每日更新...")
        
        # 4. 对这些股票统一执行一次“每日更新”
        start_date_for_retry = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
        fetch_and_insert_stocks(
            supabase_client=supabase_client,
            stock_info=retry_stock_info,
            start_date=start_date_for_retry,
            task_desc="校验修复同步"
        )

    except Exception as e:
        logging.error(f"执行校验修复时发生严重错误: {e}")
        return


def main():
    """脚本主入口，使用argparse处理命令行参数"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        logging.error("关键配置缺失：请设置 SUPABASE_URL 和 SUPABASE_KEY 环境变量。")
        raise ValueError("Missing Supabase credentials")

    parser = argparse.ArgumentParser(description="中证800股票数据同步工具")
    parser.add_argument(
        "mode",
        choices=['daily', 'full', 'partial', 'verify'],
        default='daily',
        nargs='?',
        help="选择同步模式: 'daily' (近5天), 'full' (全部历史), 'partial' (自2015年), 'verify' (校验修复)."
    )
    args = parser.parse_args()
    sync_mode = args.mode
    
    logging.info(f"--- 当前运行模式: {sync_mode} ---")
    
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    logging.info("正在获取最新的中证800成分股列表...")
    stock_info = get_csi800_stock_info()
    if not stock_info:
        logging.error("无法获取股票列表，程序终止。")
        return

    if sync_mode == 'daily':
        start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
        fetch_and_insert_stocks(supabase, stock_info, start_date, "每日增量更新")
    
    elif sync_mode == 'verify':
        verify_and_retry_sync(supabase, stock_info)

    elif sync_mode in ['full', 'partial']:
        start_dates = {'full': "20050101", 'partial': "20150101"}
        task_desc = "全量历史同步" if sync_mode == 'full' else "部分历史同步"
        fetch_and_insert_stocks(supabase, stock_info, start_dates[sync_mode], task_desc)


if __name__ == "__main__":
    main()
