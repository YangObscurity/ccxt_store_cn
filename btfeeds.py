# btfeeds.py
import sqlite3
import logging
import backtrader as bt
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class SQLiteData(bt.feeds.PandasDirectData):
    params = (
        ('dbname', None),
        ('symbol', None),
        ('timeframe', bt.TimeFrame.Minutes),
        ('compression', 1),
        ('fromdate', None),
        ('name', ''),  # 新增name参数
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.conn = None
        self.cur = None

    def start(self):
        self.conn = sqlite3.connect(self.p.dbname)
        table = 'ohlcv_1m' if self.p.timeframe == bt.TimeFrame.Minutes else 'ohlcv_1h'
        query = f'''
            SELECT timestamp, open, high, low, close, volume 
            FROM {table} 
            WHERE symbol = ? AND timestamp BETWEEN ? AND ?
            ORDER BY timestamp
        '''
        
        min_ts = int(self.p.fromdate.timestamp() * 1000) if self.p.fromdate else 1262304000000
        max_ts = 4102444800000  # 2100-01-01
        self.cur = self.conn.execute(query, (self.p.symbol, min_ts, max_ts))

    def stop(self):
        if self.conn:
            self.conn.close()

    def _load(self):
        row = self.cur.fetchone()
        if row is None:
            return False
            
        try:
            # 原始毫秒时间戳校验
            raw_ts = int(row[0])
            if not (1262304000000 <= raw_ts <= 4102444800000):
                raise ValueError(f"原始时间戳异常: {raw_ts}")

            # 转换为UTC时间
            ts = raw_ts // 1000
            if not (1262304000 <= ts <= 4102444800):
                raise ValueError(f"转换后时间戳异常: {ts}")

            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            if not (2010 <= dt.year <= 2100):
                raise ValueError(f"UTC时间超限: {dt} | 原始值: {raw_ts}")

            # Backtrader专用时间转换
            self.lines.datetime[0] = bt.date2num(dt)
            self.lines.open[0] = row[1]
            self.lines.high[0] = row[2]
            self.lines.low[0] = row[3]
            self.lines.close[0] = row[4]
            self.lines.volume[0] = row[5]
            return True
        except Exception as e:
            logger.error(f"数据拒绝: {str(e)} 行数据: {row}")
            return False