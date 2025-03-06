import sqlite3
import os
import time
import threading
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Tuple
import pandas as pd
import numpy as np
from tqdm import tqdm
import queue
import ccxt
from cachetools import TTLCache
from tenacity import retry, stop_after_attempt, wait_exponential, wait_fixed,retry_if_exception_type
from config import ConfigManager
from logger import logger
class DataManager:
    def __init__(self, store=None):
        self.store = store
        self.config = ConfigManager().config
        self.cache = TTLCache(maxsize=200, ttl=600)
        self.db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data/tick.db'))
        self.symbol_metadata = {}
        self.progress_queue = None
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        # 初始化数据库连接参数
        self.conn_params = {
            'database': self.db_path,
            'timeout': 30,
            'check_same_thread': False,
            'isolation_level': None
        }
        
        # 初始化数据库结构
        self._create_tables()
        
        # 统一锁名称
        self.write_queue = queue.Queue(maxsize=10000)
        self.hist_lock = threading.Lock()
        self.gap_lock = threading.Lock()
        self.metadata_lock = threading.RLock()
        self._start_writer_thread()
        
        
        logger.debug(f"DataManager初始化完成，加载配置项: {list(self.config.keys())}")
        
    def _create_tables(self):
        """创建数据库表结构（保留完整表结构）"""
        with sqlite3.connect(**self.conn_params) as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA busy_timeout=5000')
            
            # OHLCV表（保留分钟和小时线表）
            for tf in ['1m', '1h']:
                conn.execute(f'''
                    CREATE TABLE IF NOT EXISTS ohlcv_{tf} (
                        symbol TEXT,
                        timestamp INTEGER,
                        open REAL CHECK(open > 0),
                        high REAL CHECK(high >= open AND high >= low),
                        low REAL CHECK(low <= open AND low <= high),
                        close REAL CHECK(close > 0),
                        volume REAL CHECK(volume >= 0),
                        PRIMARY KEY(symbol, timestamp)
                    ) WITHOUT ROWID
                ''')
            # 绘图数据表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS plot_data (
                    time INTEGER,
                    symbol TEXT,
                    timeframe TEXT,
                    price REAL,
                    ma REAL,
                    rsi REAL,
                    atr REAL,
                    signals TEXT,
                    PRIMARY KEY(symbol, timeframe, time)
                )
            ''')
            # 缺口表（保留缺口检测功能）
            conn.execute('''
                CREATE TABLE IF NOT EXISTS data_gaps (
                    symbol TEXT,
                    timeframe TEXT,
                    start_ts INTEGER CHECK(start_ts > 0),
                    end_ts INTEGER CHECK(end_ts > start_ts),
                    retries INTEGER DEFAULT 0 CHECK(retries >= 0),
                    next_retry_ts INTEGER CHECK(next_retry_ts > 0),
                    PRIMARY KEY(symbol, timeframe, start_ts)
                )
            ''')
            # 同步进度表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS sync_progress (
                    symbol TEXT,
                    timeframe TEXT,
                    last_ts INTEGER,
                    PRIMARY KEY(symbol, timeframe)
                )
            ''')
            # 元数据表（新增增强型元数据管理）
            conn.execute('''
                CREATE TABLE IF NOT EXISTS symbol_metadata (
                    symbol TEXT PRIMARY KEY,
                    first_ts INTEGER CHECK(first_ts > 0),
                    last_ts INTEGER CHECK(last_ts >= first_ts),
                    list_time INTEGER CHECK(list_time > 0)
                )
            ''')
            conn.commit()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=60))
    def save_ohlcv_batch(self, symbol: str, timeframe: str, data: List[list]) -> int:
        valid = []
        min_ts = float('inf')
        max_ts = -float('inf')
        table = f'ohlcv_{timeframe}'
        chunk_size = 500
        saved = 0

        # 时间戳标准化处理
        for row in data:
            if len(row) != 6:
                continue
                
            raw_ts = row[0]
            # 自动识别时间戳格式
            if 1e9 <= raw_ts < 1e10:    # 秒级时间戳
                ts = int(raw_ts * 1000)
            elif 1e12 <= raw_ts < 1e13:  # 毫秒级时间戳
                ts = int(raw_ts)
            else:
                logger.warning(f"异常时间戳格式: {raw_ts}")
                continue

            # 数据有效性验证
            o, h, l, c, v = map(float, row[1:])
            if not (0 < o <= h and l <= c <= h and l > 0 and v >= 0):
                logger.debug(f"无效数据行: {row}")
                continue

            valid.append((symbol, ts, o, h, l, c, v))
            min_ts = min(min_ts, ts)
            max_ts = max(max_ts, ts)

        if not valid:
            logger.warning(f"无有效数据可保存: {symbol} {timeframe}")
            return 0

        # 分块写入数据库
        try:
            conn = sqlite3.connect(**self.conn_params)
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')

            for i in range(0, len(valid), chunk_size):
                chunk = valid[i:i+chunk_size]
                try:
                    # 批量插入数据
                    conn.executemany(
                        f'''INSERT OR REPLACE INTO {table}
                            (symbol, timestamp, open, high, low, close, volume)
                            VALUES (?,?,?,?,?,?,?)''',
                        chunk
                    )
                    
                    # 更新元数据
                    conn.execute('''
                        INSERT OR REPLACE INTO symbol_metadata 
                        (symbol, first_ts, last_ts, list_time)
                        VALUES (
                            ?,
                            COALESCE((SELECT MIN(first_ts) FROM symbol_metadata WHERE symbol=?), ?),
                            COALESCE((SELECT MAX(last_ts) FROM symbol_metadata WHERE symbol=?), ?),
                            (SELECT list_time FROM symbol_metadata WHERE symbol=?)
                        )
                    ''', (symbol, symbol, min_ts, symbol, max_ts, symbol))
                    
                    conn.commit()
                    saved += conn.total_changes
                    logger.debug(f"已写入 {len(chunk)} 条数据到 {table}")

                except sqlite3.IntegrityError as e:
                    logger.error(f"数据冲突: {str(e)}")
                    conn.rollback()
                except sqlite3.OperationalError as e:
                    if "locked" in str(e):
                        logger.warning("数据库锁定，等待后重试...")
                        time.sleep(0.5)
                        conn.rollback()
                        # 重试当前分块
                        i -= chunk_size  
                    else:
                        raise

            # 更新缓存
            with self.metadata_lock:
            # 直接从数据库获取最新值保证一致性
                current_first = self.get_oldest_timestamp(symbol, timeframe) or float('inf')
                current_last = self.get_last_timestamp(symbol, timeframe) or -float('inf')
                
                new_first = min(current_first, min_ts)
                new_last = max(current_last, max_ts)
                
                # 强制类型转换
                self.symbol_metadata[symbol] = {
                    'first_ts': int(new_first),
                    'last_ts': int(new_last),
                    'list_time': self.symbol_metadata.get(symbol, {}).get('list_time')
                }
                logger.debug(f"元数据更新 {symbol}: {self.symbol_metadata[symbol]}")

            logger.info(f"成功保存{symbol} {timeframe}数据 {saved}条")
            return saved

        except Exception as e:
            logger.error(f"数据库写入失败: {str(e)}", exc_info=True)
            conn.rollback()
            return saved
        finally:
            if conn:
                conn.close()

    def _validate_ohlcv_row(self, row: list) -> bool:
        if len(row) != 6:
            return False
        ts, o, h, l, c, v = row
        return (
            0 < o <= h and 
            l <= c <= h and 
            l > 0 and 
            v >= 0 and 
            1_614_393_600_000 <= ts <= 4_102_444_800_000
        )

    def _get_symbol_metadata(self, symbol: str) -> dict:
        if symbol in self.symbol_metadata:
            return self.symbol_metadata[symbol]
        
        # 增强初始化逻辑
        default_ts = int(time.time() * 1000)
        with self.metadata_lock:
            with sqlite3.connect(**self.conn_params) as conn:
                row = conn.execute('''
                    SELECT first_ts, last_ts, list_time 
                    FROM symbol_metadata 
                    WHERE symbol = ?
                ''', (symbol,)).fetchone()
                
                if row:
                    # 处理数据库NULL值
                    first_ts = row[0] if row[0] is not None else default_ts
                    last_ts = row[1] if row[1] is not None else default_ts
                    meta = {
                        'first_ts': min(first_ts, last_ts),
                        'last_ts': max(first_ts, last_ts),
                        'list_time': row[2]
                    }
                    if row[0] is None or row[1] is None:
                        logger.warning(f"修复元数据空值 {symbol}")
                else:
                    list_time = self._fetch_list_time(symbol)
                    meta = {
                        'first_ts': default_ts,
                        'last_ts': default_ts,
                        'list_time': list_time
                    }
                    conn.execute('''
                        INSERT INTO symbol_metadata 
                        (symbol, first_ts, last_ts, list_time)
                        VALUES (?, ?, ?, ?)
                    ''', (symbol, default_ts, default_ts, list_time))
                    conn.commit()
                    logger.info(f"新建元数据记录 {symbol}")
                
                # 强制类型校验
                meta['first_ts'] = int(meta['first_ts'])
                meta['last_ts'] = int(meta['last_ts'])
                self.symbol_metadata[symbol] = meta
                return meta
           
    def _fetch_list_time(self, symbol: str) -> int:
        """强化上市时间获取逻辑"""
        try:
            exchange = self.store.exchange
            markets = exchange.load_markets(reload=True)
            
            if exchange.id == 'okx':
                inst_id = symbol.replace('/', '-')
                response = exchange.publicGetPublicInstruments({
                    'instType': 'SPOT', 
                    'instId': inst_id
                })
                list_time = int(response['data'][0]['listTime'])
            else:
                market = markets.get(symbol)
                list_time = market['info'].get('listing_date', market['timestamp'])
            
            # 时间戳有效性验证
            current_ts = int(time.time() * 1000)
            if not (1000000000000 < list_time < current_ts):
                raise ValueError("Invalid listing time")
                
            return list_time
        except Exception as e:
            logger.error(f"获取上市时间失败: {symbol} {str(e)}")
            # 默认返回1年前时间戳
            return int((datetime.now(timezone.utc) - timedelta(days=365)).timestamp() * 1000)

    def fill_history(self, symbol: str, timeframe: str):
        """重构后的智能补全方法"""
        metadata = self._get_symbol_metadata(symbol)
        list_time = metadata['list_time']
        now_ts = int(time.time() * 1000)
        tf_ms = self._timeframe_to_ms(timeframe)
        
        # 动态设置时间范围
        if timeframe == '1h':
            max_days = 365
        elif timeframe == '1m':
            max_days = 30
        else:
            return

        end_ts = self._align_ts(now_ts, tf_ms)
        start_ts = end_ts - (max_days * 86400_000)
        actual_start_ts = max(list_time, start_ts)
        
        # 读取同步进度
        saved_progress = self._get_sync_progress(symbol, timeframe)
        last_in_db = self.get_last_timestamp(symbol, timeframe)
        end_ts = min(last_in_db, end_ts) if last_in_db else end_ts
        current_ts = saved_progress if saved_progress else end_ts
        
        logger.info(f"智能补全范围: {self._ts_to_str(actual_start_ts)} -> {self._ts_to_str(end_ts)}")
        
        success = False
        retry_count = 0  # 新增重试计数器
        with self.hist_lock, tqdm(desc=f"📚 {symbol} {timeframe}", unit="页") as pbar:
            try:
                while current_ts > actual_start_ts and retry_count < 5:  # 最大重试5次
                    batch_start = max(current_ts - 1000 * tf_ms, actual_start_ts)
                    
                    data = self._safe_fetch_ohlcv(
                        symbol, timeframe, 
                        since=batch_start,
                        limit=1000
                    )
                    
                    if not data:
                        retry_count += 1
                        logger.warning(f"空数据响应，重试计数: {retry_count}")
                        time.sleep(2 ** retry_count)  # 指数退避
                        continue
                    
                    retry_count = 0  # 重置计数器
                    
                    valid_data = [row for row in data if actual_start_ts <= row[0] <= end_ts]
                    if not valid_data or valid_data[0][0] >= current_ts:
                        break
                    
                    self.save_ohlcv_batch(symbol, timeframe, valid_data)
                    pbar.update(len(valid_data))
                    current_ts = self._align_ts(valid_data[0][0] - tf_ms, tf_ms)
                    self._save_progress(symbol, timeframe, current_ts)
                    
                    time.sleep(max(0.3, self.store.exchange.rateLimit / 3000))
                
                oldest = self.get_oldest_timestamp(symbol, timeframe)
                latest = self.get_last_timestamp(symbol, timeframe)
                if not oldest or not latest:
                    success = False
                else:
                    tolerance = 7 * 86400_000
                    success = (
                        (oldest <= actual_start_ts + tolerance) and 
                        (latest >= end_ts - tolerance)
                    )
            except Exception as e:
                logger.error(f"补全中断: {symbol} {timeframe} {str(e)}")
                self._save_progress(symbol, timeframe, current_ts)
                success = False
        
        if success:
            logger.info(f"补全完成: {symbol} {timeframe}")
            self._mark_as_repaired(symbol, timeframe)
        else:
            logger.warning(f"数据不完整: {symbol} {timeframe} 最新范围:{self._ts_to_str(oldest)}-{self._ts_to_str(latest)}")
    
    def _mark_as_repaired(self, symbol: str, timeframe: str):
        """标记数据已修复"""
        with sqlite3.connect(**self.conn_params) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO sync_progress 
                (symbol, timeframe, last_ts) 
                VALUES (?, ?, ?)
            ''', (symbol, timeframe, int(time.time()*1000)))
            conn.commit()

    def _save_progress(self, symbol, tf, current_ts):
        """保存补全进度"""
        with sqlite3.connect(**self.conn_params) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO sync_progress 
                (symbol, timeframe, last_ts) 
                VALUES (?, ?, ?)
            ''', (symbol, tf, current_ts))
            conn.commit()

    def _get_sync_progress(self, symbol, tf):
        """获取补全进度"""
        with sqlite3.connect(**self.conn_params) as conn:
            row = conn.execute('''
                SELECT last_ts FROM sync_progress
                WHERE symbol=? AND timeframe=?
            ''', (symbol, tf)).fetchone()
            return row[0] if row else None

    @retry(stop=stop_after_attempt(3),
       wait=wait_fixed(2),
       retry=retry_if_exception_type((ccxt.NetworkError, ccxt.ExchangeError, KeyError)))
    def _safe_fetch_ohlcv(self, symbol: str, timeframe: str, since: int, limit: int):
        """数据获取方法"""
        retries = 0
        max_retries = 3
        tf_ms = self._timeframe_to_ms(timeframe)
        
        while retries < max_retries:
            try:
                # 自动计算end时间戳
                end = since + (limit * tf_ms)
                
                # 严格对齐时间戳
                aligned_since = self._align_ts(since, tf_ms)
                aligned_end = self._align_ts(end, tf_ms)
                
                data = self.store.exchange.fetch_ohlcv(
                    symbol,
                    timeframe=timeframe,
                    since=aligned_since,
                    limit=limit,
                    params={'instType': 'SPOT'}
                )
                
                # 增强时间序列验证
                if data:
                    prev_ts = data[0][0]
                    for row in data[1:]:
                        current_ts = row[0]
                        if current_ts <= prev_ts:
                            raise ValueError(f"时间戳非递增 {prev_ts} -> {current_ts}")
                        if current_ts > aligned_end:
                            raise ValueError(f"数据超出范围 {current_ts} > {aligned_end}")
                        prev_ts = current_ts

                return data
            
            except ccxt.NetworkError as e:
                logger.warning(f"网络错误({retries+1}/{max_retries}): {str(e)}")
                time.sleep(2 ** retries)
                retries += 1
            
            except ccxt.ExchangeError as e:
                logger.warning(f"交易所错误({retries+1}/{max_retries}): {str(e)}")
                time.sleep(2 ** retries)
                retries += 1
            
            except ccxt.RateLimitExceeded as e:
                logger.warning(f"API频率限制触发，等待后重试: {str(e)}")
                time.sleep(60)
                return []
            
            except Exception as e:
                logger.error(f"数据获取失败: {str(e)}")
                raise

        return []        
    
    def _start_writer_thread(self):
        """启动异步写入线程"""
        def writer():
            while True:
                try:
                    task = self.write_queue.get(timeout=5)
                    if task is None:
                        break
                    self.save_ohlcv_batch(**task)
                except queue.Empty:
                    continue
                except Exception as e:
                    logger.error(f"写入线程异常: {str(e)}")
                    
        threading.Thread(target=writer, daemon=True, name="DBWriter").start()

    @staticmethod
    def _align_ts(ts: int, timeframe_ms: int) -> int:
        return ts if (ts % timeframe_ms == 0) else ((ts // timeframe_ms) + 1) * timeframe_ms

    @staticmethod
    def _timeframe_to_ms(timeframe: str) -> int:
        return {
            '1m': 60_000,
            '1h': 3_600_000,
            '1d': 86_400_000
        }[timeframe]

    def get_ohlcv_count(self, symbol: str, timeframe: str) -> int:
        """获取数据量"""
        table = f'ohlcv_{timeframe}'
        with sqlite3.connect(**self.conn_params) as conn:
            return conn.execute(f'SELECT COUNT(*) FROM {table} WHERE symbol = ?', (symbol,)).fetchone()[0]

    def get_last_timestamp(self, symbol: str, timeframe: str) -> Optional[int]:
        """获取最新时间戳"""
        with sqlite3.connect(**self.conn_params) as conn:
            row = conn.execute(
                f'SELECT MAX(timestamp) FROM ohlcv_{timeframe} WHERE symbol = ?',
                (symbol,)
            ).fetchone()
            return row[0] if row else None

    def check_and_fill_gaps(self, symbol: str, timeframe: str, start_ts: int = None, end_ts: int = None):
        """缺口处理"""
        if not self._should_process(symbol, timeframe, 'gap'):
            return

        try:
            with self.gap_lock:  # 使用with语句确保锁的释放
                logger.info(f"🏗️ 启动缺口扫描: {symbol} {timeframe}")
                
                # 如果未指定范围则自动检测
                if start_ts is None or end_ts is None:
                    gaps = self._precision_detect_gaps(symbol, timeframe)
                else:
                    # 直接创建指定范围的缺口
                    gaps = [(start_ts, end_ts)]
                
                # 有效性验证
                valid_gaps = []
                for start, end in gaps:
                    if start >= end:
                        continue
                    if end > int(time.time()*1000):
                        end = int(time.time()*1000)
                    valid_gaps.append((start, end))
                
                if not valid_gaps:  # 新增判断
                    logger.info("未检测到有效数据缺口")
                    return
                
                with tqdm(
                    total=len(valid_gaps),
                    desc=f"🔧 {symbol} {timeframe} 缺口修复",
                    bar_format="{desc}: {percentage:.0f}%|{bar}| {n_fmt}/{total_fmt}"
                ) as pbar:
                    success_count = 0
                    for start, end in valid_gaps:
                        if self._fill_single_gap(symbol, timeframe, start, end, pbar):
                            success_count += 1
                        pbar.update(1)
                        if self._stop_event.is_set():  # 新增退出检查
                            break
                    
                    status_msg = (
                        f"缺口处理完成 | 有效缺口: {len(valid_gaps)}个 | "
                        f"成功: {success_count}个 | 失败: {len(valid_gaps)-success_count}个"
                    )
                    logger.info(status_msg)
        except Exception as e:
            logger.error(f"缺口处理异常: {str(e)}", exc_info=True)
        finally:
            self._clean_processed_gaps(symbol, timeframe)

    def _record_failed_gaps(self, symbol: str, timeframe: str, start: int, end: int):
        """记录失败缺口"""
        with sqlite3.connect(**self.conn_params) as conn:
            conn.execute('''
                UPDATE data_gaps SET 
                    retries = retries + 1,
                    next_retry_ts = ?
                WHERE symbol=? AND timeframe=? AND start_ts=?
            ''', (int(time.time()*1000) + 3600000, symbol, timeframe, start))
            conn.commit()

    def _fill_single_gap(self, symbol, tf, start, end, pbar):
        """优化版单缺口补全"""
        tf_ms = self._timeframe_to_ms(tf)
        current = start
        success = False
        
        try:
            while current <= end:
                data = self._safe_fetch_ohlcv(symbol, tf, current, 1000)
                if not data:
                    break
                
                # 使用新的范围过滤方法
                valid = self._filter_data_by_range(data, start, end)
                if not valid:
                    break
                
                self.save_ohlcv_batch(symbol, tf, valid)
                pbar.update(len(valid))
                current = valid[-1][0] + tf_ms
                success = True
            
            return success
        except Exception as e:
            logger.error(f"补全缺口失败 {start}-{end}: {str(e)}")
            return False

    def _filter_data_by_range(self, data, start, end):
        """统一数据过滤方法"""
        return [d for d in data if start <= d[0] <= end]

    def _precision_detect_gaps(self, symbol, tf):
        """精确缺口"""
        table = f'ohlcv_{tf}'
        step = self._timeframe_to_ms(tf)
        gaps = []
        current_ts = int(time.time() * 1000)
        
        with sqlite3.connect(**self.conn_params) as conn:
            # 使用参数化查询防止SQL注入
            df = pd.read_sql(f'''
                SELECT timestamp 
                FROM {table}
                WHERE symbol = ? 
                AND timestamp <= ?
                ORDER BY timestamp
            ''', conn, params=(symbol, current_ts))
            
            if len(df) < 2:
                return []
            
            df['prev'] = df['timestamp'].shift(1)
            df['gap'] = df['timestamp'] - df['prev'] - step
            # 放宽缺口检测阈值
            anomalies = df[(df['gap'] > step * 1.05) & (df['gap'] < step * 2000)]  # 从1.1改为1.05
            
            for _, row in anomalies.iterrows():
                gap_start = int(row['prev'] + step)
                gap_end = int(row['timestamp'] - step)
                # 过滤无效小缺口
                if gap_end - gap_start < step * 2:  # 至少缺2根K线
                    continue
                gaps.append((
                    gap_start,
                    min(gap_end, current_ts)
                ))
        
        last_ts = df['timestamp'].iloc[-1]
        if last_ts < current_ts - step:
            gaps.append((
                last_ts + step,
                current_ts
            ))
        return gaps

       
    def save_plot_data(self, symbol: str, timeframe: str, data: dict):
        try:
            # 增强数据验证
            required_fields = ['time', 'price', 'ma', 'rsi', 'atr', 'signals']
            if not all(field in data for field in required_fields):
                raise ValueError("缺失必要字段")
                
            if len({len(data[f]) for f in required_fields}) != 1:
                raise ValueError("所有数据字段长度必须一致")

            df = pd.DataFrame({
                'time': pd.to_datetime(data['time'], unit='ms', utc=True),
                'symbol': symbol,
                'timeframe': timeframe,
                'price': pd.to_numeric(data['price'], errors='coerce'),
                'ma': pd.to_numeric(data['ma'], errors='coerce'),
                'rsi': pd.to_numeric(data['rsi'], errors='coerce'),
                'atr': pd.to_numeric(data['atr'], errors='coerce'),
                'signals': data['signals']
            }).dropna(subset=['price', 'ma', 'rsi'])

            # 强制类型转换
            df['time'] = df['time'].astype('datetime64[ms]')
            numeric_cols = ['price', 'ma', 'rsi', 'atr']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

            # 使用事务写入
            try:
                self.write_queue.put({
                    'type': 'plot',
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'data': df
                }, block=True, timeout=10)
            except queue.Full:
                logger.warning(f"绘图数据队列已满，丢弃{symbol} {timeframe}数据")
                
        except Exception as e:
            logger.error(f"准备绘图数据失败: {str(e)}", exc_info=True)

    def get_oldest_timestamp(self, symbol: str, timeframe: str) -> Optional[int]:
        """获取最早时间戳"""
        table = 'ohlcv_1m' if timeframe == '1m' else 'ohlcv_1h'
        with sqlite3.connect(**self.conn_params) as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA busy_timeout=5000')
            row = conn.execute(f'''
                SELECT MIN(timestamp) FROM {table}
                WHERE symbol = ?
            ''', (symbol,)).fetchone()
            return row[0] if row[0] else None
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(60))
    def sync_symbol(self, symbol: str):
        try:
            # 先执行历史数据补全
            if self.config.get('fill_history', False):
                logger.info(f"强制历史数据补全: {symbol}")
                for tf in ['1h', '1m']:  # 先小时线后分钟线
                    self.fill_history(symbol, tf)
                return True
            # 原有实时同步
            if not self.fetch_ohlcv(symbol):
                return False
            return True
        except Exception as e:
            logger.error(f"同步失败: {symbol} - {str(e)}")
            return False
    
    def _clear_existing_data(self, symbol: str, timeframe: str):
        """清除现有数据以强制重新同步"""
        table = 'ohlcv_1m' if timeframe == '1m' else 'ohlcv_1h'
        with sqlite3.connect(**self.conn_params) as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA busy_timeout=5000')
            conn.execute(f'DELETE FROM {table} WHERE symbol = ?', (symbol,))
            logger.warning(f"已清除{symbol} {timeframe}的现有数据")

    def set_progress_queue(self, queue):
        self.progress_queue = queue

    def _update_progress(self, n: int = 1):
        if self.progress:
            self.progress.update(n)
            if self.progress_queue:
                try:
                    self.progress_queue.put_nowait((
                        self.current_symbol,
                        self.current_tf,
                        self.progress.n,
                        self.progress.total
                    ))
                except queue.Full:
                    pass
    
    def ensure_data_range(self, symbol: str, timeframe: str) -> bool:
        # 精确计算毫秒时间戳
        now = int(time.time() * 1000)
        if timeframe == '1h':
            required_start = now - (365 * 86400_000)  # 365天
        elif timeframe == '1m':
            required_start = now - (30 * 86400_000)    # 30天
        else:
            return True

        # 获取实际数据边界（增强空值处理）
        oldest = self.get_oldest_timestamp(symbol, timeframe)
        latest = self.get_last_timestamp(symbol, timeframe)
        if not oldest or not latest:
            logger.error(f"获取时间戳失败，强制全量补全")
            return self._fill_range(symbol, timeframe, required_start, now)
        
        # 动态调整要求（允许7天缺口）
        tolerance = 7 * 86400_000
        success = (oldest <= required_start + tolerance) and (latest >= now - tolerance)
        
        if not success:
            logger.warning(f"数据范围异常 {symbol} {timeframe} | 现有范围: {self._ts_to_str(oldest)}-{self._ts_to_str(latest)} | 需求范围: {self._ts_to_str(required_start)}-{self._ts_to_str(now)}")
            return self._fill_range(symbol, timeframe, required_start, now)
        return True
    
    @staticmethod
    def _ts_to_str(ts: int) -> str:
        return datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M')

    def _fill_range(self, symbol: str, timeframe: str, start: int, end: int) -> bool:
        """精确填充时间范围"""
        tf_ms = self._timeframe_to_ms(timeframe)
        current = end
        total = 0
        max_attempts = 1000
        retry_count = 0
        logger.info(f"强制补全范围: {self._ts_to_str(start)}->{self._ts_to_str(end)} 共{(end-start)/tf_ms}根K线")
        
        with tqdm(total=(end - start)//tf_ms + 1,  # 修正总数计算
                 desc=f"紧急补全{symbol} {timeframe}") as pbar:
            for _ in range(max_attempts):
                if current < start:  # 修改终止条件
                    break
                
                try:
                    data = self._safe_fetch_ohlcv(
                        symbol, timeframe, 
                        current - 1000*tf_ms, 
                        1000
                    )
                    if not data:
                        if retry_count > 3:
                            break
                        retry_count += 1
                        continue
                    
                    # 扩展有效范围判断
                    valid = [d for d in data if (start - tf_ms) <= d[0] <= end]
                    if not valid:
                        break
                    
                    # 更新当前指针时应使用最小时间戳
                    current = min(d[0] for d in valid) - tf_ms
                    self.save_ohlcv_batch(symbol, timeframe, valid)
                    saved = len(valid)
                    total += saved
                    pbar.update(saved)
                    retry_count = 0
                    
                    # 动态调整请求间隔
                    delay = max(
                        self.store.exchange.rateLimit / 1000, 
                        0.5 if saved < 500 else 0.1
                    )
                    time.sleep(delay)
                    
                except Exception as e:
                    logger.error(f"补全异常: {str(e)}")
                    break

        logger.info(f"补全完成，实际需求/写入: {(end-start)//tf_ms+1}/{total}")
        return total > 0

    def _validate_timestamp_continuity(self, symbol: str, timeframe: str):
        """时间连续性校验"""
        table = f'ohlcv_{"1m" if timeframe == "1m" else "1h"}'
        expected_step = self._timeframe_to_ms(timeframe)
        
        with sqlite3.connect(**self.conn_params) as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA busy_timeout=5000')
            df = pd.read_sql(f'''
                SELECT timestamp,
                       (timestamp - LAG(timestamp) OVER (ORDER BY timestamp)) AS diff
                FROM {table}
                WHERE symbol = '{symbol}'
                ORDER BY timestamp
            ''', conn)
            
            anomalies = df[df['diff'] > (expected_step + 1000)]
            if not anomalies.empty:
                logger.warning(f"发现{len(anomalies)}处时间异常 {symbol} {timeframe}")

    def set_progress_queue(self, queue):
        """设置可视化进度队列"""
        self.progress_queue = queue

    def auto_repair_gaps(self):
        """后台自动修复缺口"""
        while True:
            try:
                for symbol in self.config['spot_symbols']:
                    for tf in ['1m', '1h']:
                        if self._precision_detect_gaps(symbol, tf) > 0:
                            self._fill_gaps(symbol, tf)
                time.sleep(3600)  # 每小时运行一次
            except Exception as e:
                logger.error(f"自动修复异常: {str(e)}")

    def _get_symbol_list_time(self, symbol: str) -> int:
        """强化上市时间获取逻辑"""
        if symbol in self.symbol_metadata:
            return self.symbol_metadata[symbol]['list_time']

        try:
            markets = self.store.exchange.load_markets(reload=True)
            market = markets.get(symbol)
            if not market:
                logger.error(f"找不到交易对: {symbol}")
                return self._default_list_time()

            # 多交易所兼容逻辑
            exchange_name = self.config['exchange']['name'].lower()
            info = market.get('info', {})
            
            # OKX特殊处理
            if exchange_name == 'okx':
                list_time = int(info.get('listTime', 0))
                if list_time == 0:
                    # 从币种信息中提取
                    inst_id = market['id']
                    response = self.store.exchange.publicGetPublicInstruments(params={
                        'instType': 'SPOT',
                        'instId': inst_id
                    })
                    list_time = int(response['data'][0]['listTime'])
            else:
                # 其他交易所处理
                list_time = market.get('timestamp', None) or info.get('listing_date', 0)
            
            # 最终校验
            current_ts = int(time.time() * 1000)
            if not (1000000000000 < list_time < current_ts):
                logger.warning(f"异常上市时间 {symbol}: {list_time}, 使用默认值")
                list_time = self._default_list_time()
            
            self.symbol_metadata[symbol] = {
                'list_time': list_time,
                'first_ts': None,
                'last_ts': None
            }
            logger.info(f"确定上市时间 {symbol}: {datetime.fromtimestamp(list_time/1000)}")
            return list_time
            
        except Exception as e:
            logger.error(f"获取上市时间失败 {symbol}: {str(e)}")
            return self._default_list_time()
        
    def _align_timestamp(self, ts: int, timeframe: str) -> int:
        """策略级时间对齐（支持多时间帧）"""
        if timeframe == '1h':
            return ts - (ts % 3_600_000)  # 整小时对齐
        elif timeframe == '1m':
            return ts - (ts % 60_000)     # 整分钟对齐
        else:
            return ts
    
    def _db_writer_loop(self):
        """专用写线程循环"""
        while True:
            task = None
            try:
                task = self.write_queue.get(timeout=5)
                with self.db_lock:
                    conn = sqlite3.connect(self.db_path, timeout=30)
                    try:
                        conn.executemany(task['query'], task['data'])
                        conn.commit()
                        logger.debug(f"批量写入完成 {len(task['data'])}条")
                    finally:
                        conn.close()
            except queue.Empty:
                continue
            except sqlite3.OperationalError as e:
                logger.error(f"数据库操作失败，等待后重试: {str(e)}")
                time.sleep(5)
            except Exception as e:
                logger.error(f"数据库写入线程异常: {str(e)}", exc_info=True)
                time.sleep(5)

            finally:
                if task is not None:
                    self.write_queue.task_done()
    
    def _safe_release_lock(self, lock: threading.Lock):
        """安全释放锁"""
        try:
            if lock.locked():
                lock.release()
        except RuntimeError as e:
            logger.warning(f"锁释放异常: {str(e)}")

    def _start_writer_thread(self):
        """统一版写入线程"""
        def writer():
            while True:
                try:
                    task = self.write_queue.get(timeout=5)
                    if task is None:  # 接收到终止信号
                        break
                        
                    # 动态处理任务类型
                    if task['type'] == 'ohlcv':
                        self._save_ohlcv_task(task)
                    elif task['type'] == 'plot':
                        self._save_plot_task(task)
                    elif task['type'] == 'sync':
                        self._handle_sync_task(task)
                        
                except queue.Empty:
                    continue
                except Exception as e:
                    logger.error(f"写入线程异常: {str(e)}", exc_info=True)

        threading.Thread(target=writer, daemon=True, name="DBWriter").start()

    def _save_ohlcv_task(self, task):
        """处理OHLCV数据写入"""
        with sqlite3.connect(**self.conn_params) as conn:
            conn.executemany(
                task['query'],
                task['data']
            )
            conn.commit()

    def _save_plot_task(self, task):
        """保存绘图数据"""
        df = task['data']
        try:
            with sqlite3.connect(**self.conn_params) as conn:
                df.to_sql('plot_data', conn, if_exists='append', index=False)
            logger.debug(f"保存绘图数据成功: {task['symbol']} {task['timeframe']} {len(df)}条")
        except sqlite3.IntegrityError:
            logger.warning(f"忽略重复绘图数据: {task['symbol']} {task['timeframe']}")
        except Exception as e:
            logger.error(f"保存绘图数据失败: {str(e)}")

    def get_ohlcv_count(self, symbol: str, timeframe: str) -> int:
        """获取指定品种的数据量"""
        table = 'ohlcv_1m' if timeframe == '1m' else 'ohlcv_1h'
        with sqlite3.connect(**self.conn_params) as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA busy_timeout=5000')
            return conn.execute(f'''
                SELECT COUNT(*) FROM {table}
                WHERE symbol = ?
            ''', (symbol,)).fetchone()[0]
 
    def _should_process(self, symbol: str, tf: str, process_type: str) -> bool:
        config = ConfigManager().config
    
        # 严格检查品种白名单
        if symbol not in config.get('spot_symbols', []):
            return False
        
        # 使用安全访问方式获取嵌套配置
        if process_type == 'gap':
            if not config.get('gap_fill', {}).get('enabled', False):
                return False
                
        elif process_type == 'historical':
            # 增加详细日志输出
            enabled = config.get('historical_fill', {}).get('enabled', False)
            logger.debug(f"历史补全检查 {symbol} {tf}: 配置状态={enabled}")
            if not enabled:
                return False
            
        return True

    def print_lock_status(self):
        """调试用锁状态打印"""
        status = {
            'hist_lock': self.hist_lock.locked(),
            'gap_lock': self.gap_lock.locked(),
            'write_queue_size': self.write_queue.qsize()
        }
        logger.debug(f"锁状态: {status}")
    
    def _fill_gaps(self, symbol: str, timeframe: str, pbar: tqdm) -> int:
        filled = 0
        try:
            table = f'ohlcv_{timeframe}'
            expected_step = self._timeframe_to_ms(timeframe)
            
            with sqlite3.connect(**self.conn_params) as conn:
                # 获取待处理缺口
                gaps = conn.execute('''
                    SELECT start_ts, end_ts FROM data_gaps
                    WHERE symbol = ? AND timeframe = ?
                    ORDER BY start_ts
                ''', (symbol, timeframe)).fetchall()
                
                for start_ts, end_ts in gaps:
                    current = start_ts
                    while current < end_ts:
                        data = self._safe_fetch_ohlcv(
                            symbol, 
                            timeframe, 
                            current, 
                            1000
                        )
                        
                        if not data:
                            break
                            
                        # 严格过滤有效数据范围
                        valid_data = [
                            row for row in data 
                            if start_ts <= row[0] <= end_ts
                        ]
                        
                        if not valid_data:
                            break
                        
                        # 批量保存数据
                        self.save_ohlcv_batch(symbol, timeframe, valid_data)
                        filled += len(valid_data)
                        pbar.update(len(valid_data))
                        
                        # 更新进度指针
                        current = valid_data[-1][0] + expected_step
                    
                    # 删除已处理缺口
                    conn.execute('''
                        DELETE FROM data_gaps 
                        WHERE symbol=? AND timeframe=? AND start_ts=?
                    ''', (symbol, timeframe, start_ts))
                    conn.commit()
                    
        except Exception as e:
            logger.error(f"补全缺口失败 {symbol} {timeframe}: {str(e)}")
            return filled
        return filled
    
    def _default_list_time(self) -> int:
        return int((datetime.now(timezone.utc) - timedelta(days=365)).timestamp() * 1000)

    def _close_db_connections(self):
        if hasattr(self, 'conn') and self.conn:
            try:
                if self.conn.in_transaction:
                    self.conn.commit()  # 或rollback根据需求
                self.conn.close()
                logger.debug("数据库连接已安全关闭")
            except Exception as e:
                logger.error(f"关闭连接失败: {str(e)}")
            finally:
                self.conn = None

    def get_last_timestamp(self, symbol: str, timeframe: str) -> Optional[int]:
        table = f'ohlcv_{timeframe}'
        with sqlite3.connect(**self.conn_params) as conn:
            row = conn.execute(
                f'SELECT MAX(timestamp) FROM {table} WHERE symbol = ?',
                (symbol,)
            ).fetchone()
            return row[0] if row else None

    def _get_last_timestamp(self, symbol: str) -> int:
        with sqlite3.connect(**self.conn_params) as conn:
            cur = conn.execute('''
                SELECT MAX(timestamp) FROM ohlcv_1m WHERE symbol = ?
            ''', (symbol,))
            result = cur.fetchone()[0]
            return result or int((datetime.now() - timedelta(days=30)).timestamp() * 1000)

    def _safe_fetch_with_progress(self, symbol, tf, since, limit, pbar):
        """带进度跟踪的数据获取"""
        try:
            data = self._safe_fetch_ohlcv(symbol, tf, since, limit)
            if not data:
                return None
                
            valid = [row for row in data if row[0] > since]
            if valid:
                self.save_ohlcv_batch(symbol, tf, valid)
                pbar.update(len(valid))
            return valid
        except Exception as e:
            logger.error(f"获取失败: {symbol} {tf} {str(e)}")
            return None

    def _fill_single_gap(self, symbol, tf, start, end, pbar):
        """单缺口补全"""
        tf_ms = self._timeframe_to_ms(tf)
        current = start
        success = False
        
        try:
            # 计算需要获取的K线数量
            limit = ((end - start) // tf_ms) + 1  # 动态计算limit
            
            while current <= end:
                # 精确对齐时间戳
                aligned_current = self._align_ts(current, tf_ms)
                
                data = self._safe_fetch_ohlcv(
                    symbol, 
                    tf,
                    since=aligned_current,
                    limit=min(limit, 1000)  # 控制单次请求量
                )
                
                if not data:
                    break
                
                # 严格过滤在缺口范围内的数据
                valid = [row for row in data if start <= row[0] <= end]
                if not valid:
                    break
                    
                # 保存数据并更新进度
                saved = self.save_ohlcv_batch(symbol, tf, valid)
                if saved > 0:
                    pbar.update(saved)
                    current = valid[-1][0] + tf_ms
                    success = True
                else:
                    break
                
                # 更新剩余需要获取的数量
                limit -= len(valid)
                
        except Exception as e:
            logger.error(f"补全缺口失败 {start}-{end}: {str(e)}")
            return False
        
        # 二次验证缺口是否填补
        with sqlite3.connect(**self.conn_params) as conn:
            gap_count = conn.execute('''
                SELECT COUNT(*) FROM data_gaps 
                WHERE symbol=? AND timeframe=? 
                AND start_ts=? AND end_ts=?
            ''', (symbol, tf, start, end)).fetchone()[0]
            
        return gap_count == 0 and success
    
    def _clean_processed_gaps(self, symbol: str, timeframe: str):
        """清理已处理或过期的缺口记录"""
        try:
            with sqlite3.connect(**self.conn_params) as conn:
                # 删除超过3次重试的缺口记录
                conn.execute('''
                    DELETE FROM data_gaps 
                    WHERE symbol = ? 
                    AND timeframe = ?
                    AND retries >= 3
                ''', (symbol, timeframe))
                
                # 删除已经不存在时间范围内的缺口（防止残留）
                conn.execute('''
                    DELETE FROM data_gaps 
                    WHERE symbol = ? 
                    AND timeframe = ?
                    AND end_ts < (
                        SELECT MIN(timestamp) 
                        FROM ohlcv_''' + timeframe + '''
                        WHERE symbol = ?
                    )
                ''', (symbol, timeframe, symbol))
                
                conn.commit()
                logger.debug(f"清理完成 {symbol} {timeframe} 的过期缺口记录")
        except Exception as e:
            logger.error(f"清理缺口记录失败: {str(e)}")

    def _convert_db_timestamp(self, ts: int) -> int:
        """处理不同来源的时间戳格式"""
        if 1e12 <= ts < 1e13:  # 毫秒级时间戳 (13位)
            return ts
        elif 1e9 <= ts < 1e10:  # 秒级时间戳 (10位)
            return ts * 1000
        else:
            logger.warning(f"异常时间戳格式: {ts}")
            return int(time.time() * 1000)  # 返回当前时间作为默认值
        
    def _validate_last_n_bars(self, symbol: str, timeframe: str, n: int = 3):
        """验证最后n根K线的连续性"""
        table = f'ohlcv_{timeframe}'
        tf_ms = self._timeframe_to_ms(timeframe)
        
        with sqlite3.connect(**self.conn_params) as conn:
            df = pd.read_sql_query(f'''
                SELECT timestamp 
                FROM {table} 
                WHERE symbol = ? 
                ORDER BY timestamp DESC 
                LIMIT {n}
            ''', conn, params=(symbol,))
            
        if len(df) < n:
            return
        
        # 检查时间间隔
        diffs = df['timestamp'].diff().abs().iloc[1:]
        anomalies = diffs[diffs != tf_ms]
        
        if not anomalies.empty:
            logger.warning(f"发现近期数据异常 {symbol} {timeframe}")
            # 触发紧急补全
            start = df['timestamp'].min() - tf_ms * 10  # 多补10根确保连续
            end = df['timestamp'].max() + tf_ms
            self.check_and_fill_gaps(symbol, timeframe, start, end)