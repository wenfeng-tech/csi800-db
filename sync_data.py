import os
import time
import akshare as ak
import pandas as pd
from supabase import create_client, Client
from datetime import datetime

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
    sync_mode = 'daily' # <-- 在此切换模式: 'full' 或 'daily'

    print(f"当前运行模式: {sync_mode}")
    if sync_mode == 'full':
        sync_full_history(supabase)
    elif sync_mode == 'daily':
        sync_daily_update(supabase)
    else:
        print(f"错误：无法识别的模式 '{sync_mode}'。请选择 'full' 或 'daily'。")

def get_csi800_stocks() -> list:
    """获取最新的中证800成分股列表"""
    try:
        stock_df = ak.index_stock_cons_df(symbol="000906")
        print(f"成功获取中证800成分股，共 {len(stock_df)} 只股票。")
        return stock_df['品种代码'].tolist()
    except Exception as e:
        print(f"错误：获取中证800成分股列表失败 - {e}")
        return []

def get_stock_history(stock_code: str, start_date: str) -> pd.DataFrame:
    """获取单只股票从指定日期开始的前复权日线数据"""
    try:
        stock_hist_df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, adjust="qfq")
        if stock_hist_df.empty:
            return pd.DataFrame()

        stock_hist_df.rename(columns={
            '日期': 'trade_date', '开盘': 'open', '收盘': 'close', '最高': 'high',
            '最低': 'low', '成交量': 'volume', '成交额': 'turnover', '振幅': 'amplitude',
            '涨跌幅': 'change_pct', '涨跌额': 'change_amt', '换手率': 'turnover_rate'
        }, inplace=True)
        stock_hist_df['stock_code'] = stock_code
        required_columns = [
            'trade_date', 'stock_code', 'open', 'high', 'low', 'close',
            'volume', 'turnover', 'amplitude', 'change_pct', 'change_amt', 'turnover_rate'
        ]
        return stock_hist_df[required_columns]
    except Exception as e:
        print(f"错误：获取股票 {stock_code} 的历史数据失败 - {e}")
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
        time.sleep(1) # 礼貌性延时，避免IP被封
    print("🎉 全量历史数据同步完成！")

def sync_daily_update(supabase_client: Client):
    """【每日更新模式】仅同步最近5天的交易数据，用于日常维护。"""
    print("开始执行每日增量更新...")
    stock_codes = get_csi800_stocks()
    start_date = (datetime.now() - pd.Timedelta(days=5)).strftime('%Y%m%d')

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
