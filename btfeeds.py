# btfeeds.py
import sqlite3
import logging
import backtrader as bt
from datetime import datetime, timezone
logger = logging.getLogger(__name__)
import pandas as pd
import time

class SQLiteData(bt.feeds.PandasDirectData):
    params = (
        ('dbname', None),
        ('symbol', None),
        ('timeframe', bt.TimeFrame.Minutes),
        ('compression', 1),
        ('fromdate', None),
        ('name', ''),
        ('datetime', None),
        ('open', 0),
        ('high', 1),
        ('low', 2),
        ('close', 3),
        ('volume', 4),
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.conn = None
        self.cur = None
        self.dataname = pd.DataFrame()

    def start(self):
        try:
            self.conn = sqlite3.connect(self.p.dbname)
            table = 'ohlcv_1m' if self.p.timeframe == bt.TimeFrame.Minutes else 'ohlcv_1h'
            
            # 增强表存在性检查
            table_exists = self.conn.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
            ).fetchone()
            if not table_exists:
                raise ValueError(f"Table {table} not found")

            # 加载数据
            query = f'''
                SELECT timestamp, open, high, low, close, volume 
                FROM {table} 
                WHERE symbol = ? AND timestamp BETWEEN ? AND ?
                ORDER BY timestamp
            '''
            min_ts = int(self.p.fromdate.timestamp() * 1000) if self.p.fromdate else 1262304000000
            max_ts = 4102444800000
            df = pd.read_sql_query(query, self.conn, params=(self.p.symbol, min_ts, max_ts))
            
            # 数据有效性验证
            if df.empty:
                raise ValueError("Empty DataFrame")
                
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df.set_index('datetime', inplace=True)
            self.dataname = df

        except Exception as e:
            logger.error(f"数据加载失败: {str(e)}")
            self.dataname = pd.DataFrame()

    def stop(self):
        if self.conn:
            self.conn.close()

    def _load(self):
        try:
            self.conn = sqlite3.connect(self.p.dbname, timeout=30)
            table = 'ohlcv_1m' if self.p.compression == 1 else 'ohlcv_1h'
            # 从类参数中获取时间范围
            start_ts = int(self.p.fromdate.timestamp() * 1000)
            end_ts = int(self.p.todate.timestamp() * 1000)
            # 精确时间范围查询（毫秒级）
            query = f'''
                SELECT 
                    open, high, low, close, volume, timestamp 
                FROM {table} 
                WHERE symbol = ?
                ORDER BY timestamp
            '''
            params = (self.p.symbol, start_ts, end_ts)
            logger.debug(f"执行SQL查询: {query} | 参数: {params}")
            
            df = pd.read_sql_query(query, self.conn, params=params)
            logger.debug(f"实际加载数据量: {len(df)} 条")

            # 转换时间戳并设为索引
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df.set_index('datetime', inplace=True)
            if df.empty:
                logger.warning("空数据集")
                self.dataname = pd.DataFrame()  # 确保 dataname 是合法空 DataFrame
            else:
                self.dataname = df[['open', 'high', 'low', 'close', 'volume']]
            self.dataname = df[['open', 'high', 'low', 'close', 'volume']]
            
        except Exception as e:
            logger.error(f"数据加载失败: {str(e)}")
            self.dataname = pd.DataFrame()
        finally:
            if self.conn:
                self.conn.close()