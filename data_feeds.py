from typing import Optional
import backtrader as bt
import pandas as pd
import sqlite3
from datetime import datetime, timedelta, timezone
from logger import logger

class SQLiteData(bt.feeds.PandasDirectData):
    params = (
        ('dbname', None),
        ('symbol', None),
        ('timeframe', bt.TimeFrame.Minutes),
        ('compression', 1),
        ('fromdate', None),
    )

    def __init__(self):
        try:
            self._load_data()
            if self.dataname.empty:
                raise ValueError("No data available")
        except ValueError as e:
            logging.error(f"数据加载失败: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"数据加载失败: {str(e)}")
            raise

    def _load_data(self):
        table = 'ohlcv_1m' if self.p.timeframe == bt.TimeFrame.Minutes else 'ohlcv_1h'
        query = f'''
            SELECT timestamp/1000 AS datetime, open, high, low, close, volume 
            FROM {table} 
            WHERE symbol = ? 
            AND timestamp BETWEEN ? AND ?
            ORDER BY timestamp
        '''
        params = (
            self.p.symbol,
            int(self.p.fromdate.timestamp() * 1000),
            int(self.p.todate.timestamp() * 1000)
        )

    def _load_datafeed(self, symbol: str, timeframe: str, days: int) -> Optional[bt.feeds.PandasDirectData]:
        """返回有效数据或 None"""
        try:
            fromdate = datetime.now() - timedelta(days=days)
            data_name = f"{symbol}_{timeframe}"
            df = pd.read_sql_query(query, conn, params=params)
            # 显式设置时区
            df.index = pd.to_datetime(df['datetime'], unit='s', utc=True)
            df = df.tz_localize('UTC')
            table = 'ohlcv_1m' if self.p.compression == 1 else 'ohlcv_1h'
            data = SQLiteData(
                dbname=self.data_manager.db_path,
                symbol=symbol,
                timeframe=bt.TimeFrame.Minutes,
                compression=60 if timeframe == '1h' else 1,
                fromdate=fromdate,
                name=data_name,
                sessionend=datetime(1970, 1, 1),
                tz=timezone.utc  # 修正时区设置
            )
            
            # 放宽连续性校验
            if len(df) < 100:
                logger.warning(f"数据不足: {self.p.symbol} {table} ({len(df)}/100)")
                raise ValueError("Insufficient data")
                
            # 允许最多10%的缺口
            time_diff = df.index[-1] - df.index[0]
            expected = pd.Timedelta(minutes=len(df)) * 0.9
            if time_diff < expected:
                logger.error(f"时间缺口过大: {self.p.symbol} {table}")
                raise ValueError("Data continuity issue")
                
            self.dataname = df
                
            return data
        except ValueError as e:
            logger.error(f"空数据集: {symbol} {timeframe} - {str(e)}")
            return None
        except Exception as e:  # 捕获其他异常
            logger.error(f"数据加载失败: {symbol} {timeframe} - {str(e)}")
            return None
