# data_feeds.py
from typing import Optional
import backtrader as bt
import pandas as pd
import sqlite3
from datetime import datetime, timedelta, timezone
from logger import logger
import logging

class SQLiteData(bt.feeds.PandasDirectData):
    params = (
        ('dbname', None),
        ('symbol', None),
        ('timeframe', bt.TimeFrame.Minutes),
        ('compression', 1),
        ('fromdate', None),
        ('todate', None),
        ('name', ''),
        ('datetime', 0),
        ('open', 1),
        ('high', 2),
        ('low', 3),
        ('close', 4),
        ('volume', 5)
    )

    def __init__(self, **kwargs):
        if 'timeframe' in kwargs:
            kwargs['timeframe'] = self._convert_timeframe(kwargs['timeframe'])
        super().__init__(**kwargs)
        self.conn = None
        self._load()

    def _convert_timeframe(self, tf):
        return {
            '1m': bt.TimeFrame.Minutes,
            '1h': bt.TimeFrame.Minutes
        }.get(tf, bt.TimeFrame.Minutes)

    def _load(self):
        try:
            self.conn = sqlite3.connect(self.p.dbname)
            table = 'ohlcv_1m' if self.p.compression == 1 else 'ohlcv_1h'
            
            # 精确转换时间范围
            from_ts = int(self.p.fromdate.timestamp() * 1000)
            to_ts = int(self.p.todate.timestamp() * 1000)
            
            query = f'''
                SELECT timestamp, open, high, low, close, volume 
                FROM {table} 
                WHERE symbol = ? 
                AND timestamp BETWEEN ? AND ?
                ORDER BY timestamp
            '''
            params = (self.p.symbol, from_ts, to_ts)
            
            df = pd.read_sql_query(query, self.conn, params=params)
            if df.empty:
                raise ValueError(f"Empty DataFrame between {from_ts} and {to_ts}")
            
            # 增强时间序列校验
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df = df.sort_values('datetime').reset_index(drop=True)
            
            # 检查连续性
            time_diff = df['datetime'].diff().dt.total_seconds() * 1000
            expected_interval = 60000 if '1m' in table else 3600000
            gaps = time_diff[time_diff > expected_interval * 1.1]
            
            if not gaps.empty:
                logger.warning(f"发现{len(gaps)}处数据缺口")
                
            self.dataname = df[['open', 'high', 'low', 'close', 'volume']]
            
        except Exception as e:
            logger.error(f"数据加载失败: {str(e)}")
            self.dataname = pd.DataFrame()
        finally:
            if self.conn:
                self.conn.close()