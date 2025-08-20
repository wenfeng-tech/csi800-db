import os
import time
import akshare as ak
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta

# --- 配置区域 ---
# 从环境变量中读取 Supabase 配置，这是最佳安全实践
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") # 此处应配置为 service_role key

# --- 主逻辑 ---
def main():
    """脚本主入口"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("关键配置缺失：请设置 SUPABASE_URL 和 SUPABASE_KEY 环境变量。")

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    sync_mode = 'daily' # <-- 在此切换模式: 'full' 或 'daily'

    print(f"当前运行模式: {sync_mode}")
    if sync_mode == 'full':
        sync_full_history(supabase)
    elif sync_mode == 'daily':
        sync_daily_update(supabase)
    else:
        print(f"错误：无法识别的模式 '{sync_mode}'。请选择 'full' 或 'daily'。")

def get_csi800_stock_info() -> dict:
    """
    【变更点】获取最新的中证800成分股代码和对应的公司名称
    返回一个字典，格式为: {'代码': '公司名称', ...}
    """
    try:
        stock_df = ak.index_stock_cons_csindex(symbol="000906") 
        print(f"成功从中证指数官网获取中证800成分股，共 {len(stock_df)} 只股票。")
        # 将两列转换为一个 code -> name 的映射字典
        return pd.Series(stock_df['成分券名称'].values, index=stock_df['成分券代码']).to_dict()
    except Exception as e:
        print(f"错误：获取中证800成分股列表失败 - {e}")
        return {}

def get_stock_history(stock_code: str, stock_name: str, start_date: str) -> pd.DataFrame:
    """
    【变更点】获取单只股票历史数据，并只保留指定列
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            stock_hist_df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, adjust="qfq")
            
            if not stock_hist_df.empty:
                # 1. 只重命名需要的列
                stock_hist_df.rename(columns={
                    '日期': 'trade_date', '开盘': 'open', '收盘': 'close', '最高': 'high',
                    '最低': 'low', '成交量': 'volume'
                }, inplace=True)
                
                # 2. 增加股票代码和名称列
                stock_hist_df['stock_code'] = stock_code
                stock_hist_df['stock_name'] = stock_name
                
                # 3. 转换日期格式以兼容JSON
                stock_hist_df['trade_date'] = pd.to_datetime(stock_hist_df['trade_date']).dt.strftime('%Y-%m-%d')
                
                # 4. 定义并筛选出最终需要的列
                required_columns = [
                    'trade_date', 'stock_code', 'stock_name', 
                    'open', 'high', 'low', 'close', 'volume'
                ]
                return stock_hist_df[required_columns]
            else:
                return pd.DataFrame()

        except Exception as e:
            print(f"警告：获取股票 {stock_code} 数据失败 (第 {attempt + 1}/{max_retries} 次尝试) - {e}")
            if attempt < max_retries - 1:
                time.sleep((attempt + 1) * 2) 
            else:
                print(f"错误：获取股票 {stock_code} 的历史数据失败，已达最大重试次数。")
                return pd.DataFrame()
    
    return pd.DataFrame()

def sync_full_history(supabase_client: Client):
    """【全量同步模式】同步所有成分股自上市以Lai的全部历史数据。"""
    print("开始执行全量历史数据同步...")
    stock_info = get_csi800_stock_info()
    start_date = "20050101"

    # 【变更点】遍历字典的键值对
    for i, (code, name) in enumerate(stock_info.items()):
        print(f"--- 全量处理: {i+1}/{len(stock_info)} | 股票代码: {code} ({name}) ---")
        hist_df = get_stock_history(code, name, start_date)
        
        if not hist_df.empty:
            data_to_insert = hist_df.to_dict(orient='records')
            try:
                supabase_client.table("csi800_daily_data").upsert(data_to_insert).execute()
                print(f"成功同步股票 {code} ({name}) 的 {len(data_to_insert)} 条历史数据。")
            except Exception as e:
                print(f"数据库错误：插入股票 {code} ({name}) 数据失败 - {e}")
        
        time.sleep(0.5)
        
    print("🎉 全量历史数据同步完成！")

def sync_daily_update(supabase_client: Client):
    """【每日更新模式】仅同步最近5天的交易数据，用于日常维护。"""
    print("开始执行每日增量更新...")
    stock_info = get_csi800_stock_info()
    start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')

    for i, (code, name) in enumerate(stock_info.items()):
        print(f"--- 增量处理: {i+1}/{len(stock_info)} | 股票代码: {code} ({name}) ---")
        hist_df = get_stock_history(code, name, start_date)
        
        if not hist_df.empty:
            data_to_insert = hist_df.to_dict(orient='records')
            try:
                supabase_client.table("csi800_daily_data").upsert(data_to_insert).execute()
                print(f"成功同步股票 {code} ({name}) 的 {len(data_to_insert)} 条近期数据。")
            except Exception as e:
                print(f"数据库错误：插入股票 {code} ({name}) 数据失败 - {e}")
        
        time.sleep(0.5)
        
    print("🚀 每日增量更新完成！")

if __name__ == "__main__":
    main()
