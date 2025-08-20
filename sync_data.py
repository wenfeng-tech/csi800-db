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

    # --- 模式选择 ---
    # 首次在本地运行时，请使用 'full' 模式
    # 部署到 GitHub Actions 后，请改为 'daily' 模式
    sync_mode = 'full' # <-- 在此切换模式: 'full' 或 'daily'

    print(f"当前运行模式: {sync_mode}")
    if sync_mode == 'full':
        sync_full_history(supabase)
    elif sync_mode == 'daily':
        sync_daily_update(supabase)
    else:
        print(f"错误：无法识别的模式 '{sync_mode}'。请选择 'full' 或 'daily'。")

def get_csi800_stocks() -> list:
    """
    【修正1】获取最新的中证800成分股列表 (使用中证指数官网接口)
    - 使用官方推荐的 ak.index_stock_cons_csindex 接口，数据更可靠。
    - 修正了获取股票代码的列名为 '成分券代码'。
    """
    try:
        stock_df = ak.index_stock_cons_csindex(symbol="000906") 
        print(f"成功从中证指数官网获取中证800成分股，共 {len(stock_df)} 只股票。")
        return stock_df['成分券代码'].tolist() 
    except Exception as e:
        print(f"错误：获取中证800成分股列表失败 - {e}")
        return []

def get_stock_history(stock_code: str, start_date: str) -> pd.DataFrame:
    """
    【修正2 & 3】获取单只股票从指定日期开始的前复权日线数据
    - 新增了重试逻辑，以应对临时的网络连接问题。
    - 新增了日期格式化，将日期对象转换为字符串，以解决JSON序列化错误。
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            stock_hist_df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, adjust="qfq")
            
            if not stock_hist_df.empty:
                stock_hist_df.rename(columns={
                    '日期': 'trade_date', '开盘': 'open', '收盘': 'close', '最高': 'high',
                    '最低': 'low', '成交量': 'volume', '成交额': 'turnover', '振幅': 'amplitude',
                    '涨跌幅': 'change_pct', '涨跌额': 'change_amt', '换手率': 'turnover_rate'
                }, inplace=True)
                stock_hist_df['stock_code'] = stock_code
                
                # --- 核心修正3：将日期对象转换为 'YYYY-MM-DD' 格式的字符串 ---
                stock_hist_df['trade_date'] = pd.to_datetime(stock_hist_df['trade_date']).dt.strftime('%Y-%m-%d')
                
                required_columns = [
                    'trade_date', 'stock_code', 'open', 'high', 'low', 'close',
                    'volume', 'turnover', 'amplitude', 'change_pct', 'change_amt', 'turnover_rate'
                ]
                return stock_hist_df[required_columns]
            else:
                # 若akshare返回空DataFrame（例如新股无历史数据），直接返回
                return pd.DataFrame()

        except Exception as e:
            print(f"警告：获取股票 {stock_code} 数据失败 (第 {attempt + 1}/{max_retries} 次尝试) - {e}")
            if attempt < max_retries - 1:
                # 逐渐增加等待时间后重试
                time.sleep((attempt + 1) * 2) 
            else:
                print(f"错误：获取股票 {stock_code} 的历史数据失败，已达最大重试次数。")
                return pd.DataFrame()
    
    return pd.DataFrame()

def sync_full_history(supabase_client: Client):
    """【全量同步模式】同步所有成分股自上市以来的全部历史数据。"""
    print("开始执行全量历史数据同步...")
    stock_codes = get_csi800_stocks()
    start_date = "19900101"

    for i, code in enumerate(stock_codes):
        print(f"--- 全量处理: {i+1}/{len(stock_codes)} | 股票代码: {code} ---")
        hist_df = get_stock_history(code, start_date)
        
        if not hist_df.empty:
            data_to_insert = hist_df.to_dict(orient='records')
            try:
                # Upsert 操作会根据主键自动判断是插入还是更新
                supabase_client.table("csi800_daily_data").upsert(data_to_insert).execute()
                print(f"成功同步股票 {code} 的 {len(data_to_insert)} 条历史数据。")
            except Exception as e:
                print(f"数据库错误：插入股票 {code} 数据失败 - {e}")
        
        # 延长延迟，降低被服务器封禁IP的风险
        time.sleep(2) 
        
    print("🎉 全量历史数据同步完成！")

def sync_daily_update(supabase_client: Client):
    """【每日更新模式】仅同步最近5天的交易数据，用于日常维护。"""
    print("开始执行每日增量更新...")
    stock_codes = get_csi800_stocks()
    start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')

    for i, code in enumerate(stock_codes):
        print(f"--- 增量处理: {i+1}/{len(stock_codes)} | 股票代码: {code} ---")
        hist_df = get_stock_history(code, start_date)
        
        if not hist_df.empty:
            data_to_insert = hist_df.to_dict(orient='records')
            try:
                supabase_client.table("csi800_daily_data").upsert(data_to_insert).execute()
                print(f"成功同步股票 {code} 的 {len(data_to_insert)} 条近期数据。")
            except Exception as e:
                print(f"数据库错误：插入股票 {code} 数据失败 - {e}")
        
        time.sleep(0.5)
        
    print("🚀 每日增量更新完成！")

if __name__ == "__main__":
    main()
