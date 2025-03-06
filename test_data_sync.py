# test_data_sync.py
import ccxt
import sys
import os
import sqlite3
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib/bt-ccxt-store-cn'))
from ccxtbt import CCXTStore
from datetime import datetime, timedelta, timezone
from data_manager import DataManager
import time
from logger import logger
from btfeeds import SQLiteData
import pandas as pd

# 配置绝对路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'data/tick.db')
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def print_table_structure(db_path):
    """打印表结构用于调试"""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print("\n数据库表结构:")
        for table in tables:
            print(f"\nTable: {table[0]}")
            cursor.execute(f"PRAGMA table_info({table[0]})")
            print(cursor.fetchall())

def debug_print(msg):
    print(f"[DEBUG] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}")

def test_btc_data():
    # 初始化交易所和数据管理器
    exchange = CCXTStore(
        exchange='okx',
        currency='USDT',
        retries=5,
        config={
            'apiKey': "xx",
            'secret': "xx",
            'password': "xx",
            'enableRateLimit': True,
            'proxies': {
                'http': 'http://address:port',
                'https': 'http://address:port'
            },
        }
    )
    exchange.exchange.verbose = True
    
    # 初始化DataManager并启用调试输出
    data_manager = DataManager(exchange)
    data_manager.progress_queue = None  # 禁用进度队列

    # 清空测试数据
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM ohlcv_1m WHERE symbol='BTC/USDT'")
        conn.execute("DELETE FROM plot_data WHERE symbol='BTC/USDT'")
        conn.commit()

    # 步骤1: 获取实时数据并保存到数据库
    print("\n=== 阶段1: 数据获取与存储 ===")
    try:
        debug_print("从交易所获取最新K线数据...")
        ohlcv = exchange.exchange.fetch_ohlcv('BTC/USDT', '1m', limit=5)
        
        if not ohlcv:
            raise ValueError("未获取到K线数据")
            
        # 数据标准化处理
        valid_data = []
        for row in ohlcv:
            if len(row) != 6:
                continue
            ts = int(row[0])
            if not (1609459200000 < ts < int(time.time() * 1000) + 86400000):
                continue
            valid_data.append(row)
            debug_print(f"原始数据: {row}")

        # 保存到数据库
        saved = data_manager.save_ohlcv_batch('BTC/USDT', '1m', valid_data)
        print(f"\n[INFO] 保存{saved}条数据到数据库")

    except Exception as e:
        print(f"\n[ERROR] 数据获取失败: {str(e)}")
        return

    # 步骤2: 验证数据库记录
    print("\n=== 阶段2: 数据库验证 ===")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql_query(
                "SELECT * FROM ohlcv_1m WHERE symbol='BTC/USDT' ORDER BY timestamp DESC LIMIT 5",
                conn
            )
            if df.empty:
                raise ValueError("数据库查询结果为空")
            print("\n数据库最新5条记录:")
            print(df.to_string(index=False))
            
            # 验证时间戳连续性
            timestamps = df['timestamp'].tolist()
            gaps = [timestamps[i] - timestamps[i+1] for i in range(len(timestamps)-1)]
            debug_print(f"时间戳间隔: {gaps} (预期应为60000ms)")

    except Exception as e:
        print(f"\n[ERROR] 数据库验证失败: {str(e)}")
        return

    # 步骤3: 加载到Backtrader并验证
    print("\n=== 阶段3: Backtrader数据加载 ===")
    try:
        debug_print("初始化Backtrader数据源...")
        fromdate = datetime.now(timezone.utc) - timedelta(hours=24)
        data = SQLiteData(
            dbname=DB_PATH,
            symbol='BTC/USDT',
            timeframe='1m',
            fromdate=fromdate,
            compression=1
        )
        data.start()
        
        if data.dataname.empty:
            raise ValueError("Backtrader数据加载为空")
            
        print("\nBacktrader加载的数据:")
        print(data.dataname.head())
        
        # 增强时间序列验证
        diffs = data.dataname.index.to_series().diff().dropna()
        debug_print(f"时间间隔分布:\n{diffs.value_counts()}")
        if any(diffs > pd.Timedelta(minutes=2)):
            print("\n[WARNING] 发现数据缺口")
        else:
            print("\n[SUCCESS] 时间序列连续")

    except Exception as e:
        print(f"\n[ERROR] Backtrader加载失败: {str(e)}")
        return

    # 步骤4: 测试绘图数据生成
    print("\n=== 阶段4: 绘图功能验证 ===")
    try:
        debug_print("生成测试绘图数据...")
        plot_data = {
            'time': [int(time.time()*1000 - i*60000) for i in range(5)][::-1],
            'price': [91306.7, 91388.5, 91334.0, 91388.4, 91484.5],
            'ma': [91200.0, 91300.0, 91350.0, 91400.0, 91450.0],
            'rsi': [35.2, 40.1, 38.5, 42.3, 45.0],
            'atr': [150.3, 148.7, 152.1, 149.5, 151.2],
            'signals': ['BUY', None, None, 'SELL', None]
        }
        
        # 强制同步写入并等待完成
        data_manager.write_queue.put({
            'type': 'plot',
            'symbol': 'BTC/USDT',
            'timeframe': '1m',
            'data': pd.DataFrame({
                'time': pd.to_datetime(plot_data['time'], unit='ms', utc=True),
                'symbol': 'BTC/USDT',          # 显式添加symbol列
                'timeframe': '1m',             # 显式添加timeframe列
                'price': plot_data['price'],
                'ma': plot_data['ma'],
                'rsi': plot_data['rsi'],
                'atr': plot_data['atr'],
                'signals': plot_data['signals']
            })
        }, block=True, timeout=10)
        time.sleep(1)  # 等待异步写入完成
        
        # 验证绘图数据
        with sqlite3.connect(DB_PATH) as conn:
            plot_df = pd.read_sql_query(
                "SELECT * FROM plot_data WHERE symbol='BTC/USDT' ORDER BY time DESC LIMIT 5",
                conn
            )
            print("\n绘图数据验证:")
            print(plot_df.to_string(index=False))
            
            if plot_df.empty:
                raise ValueError("绘图数据未保存")
            else:
                debug_print("数据字段检查:")
                debug_print(f"Symbol列值: {set(plot_df['symbol'])}")
                debug_print(f"Timeframe列值: {set(plot_df['timeframe'])}")
                print("\n[SUCCESS] 绘图数据保存正常")

    except Exception as e:
        print(f"\n[ERROR] 绘图功能异常: {str(e)}")
        return

    print("\n=== 所有测试通过 ===")

if __name__ == "__main__":
    print("="*50)
    print(" 量化系统集成测试启动")
    print(f" 数据库路径: {DB_PATH}")
    print("="*50)
    test_btc_data()