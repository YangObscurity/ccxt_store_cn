import time
import threading
import signal
import gc
import os
import sys
from datetime import datetime, timedelta, timezone
import backtrader as bt
import pandas as pd
import ccxt
from logger import logger
from data_manager import DataManager
from visualizer import StrategyVisualizer
from config import ConfigManager
from data_feeds import SQLiteData
from typing import Optional
from database import database_maintenance
from strategies.query_strategy import EnhancedQueryStrategy
from threading import Thread
import sqlite3
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib/bt-ccxt-store-cn'))
from ccxtbt import CCXTStore

class TradingMonitor:
    def __init__(self):
        self.config = ConfigManager().config
        self.cerebro = bt.Cerebro(
            quicknotify=True,
            exactbars=1,
            stdstats=False,
            live=True
        )
        self.store = None
        self.data_manager = None
        self.visualizer = None
        self._stop_event = threading.Event()
        self._setup_signal_handlers()
        self.active_threads = {}
        self.enable_visualization = self.config.get('enable_visualization', True)

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self.safe_shutdown)
        signal.signal(signal.SIGTERM, self.safe_shutdown)

    def setup_exchange(self):
        exchange_config = {
            'apiKey': self.config["exchange"]["apikey"],
            'secret': self.config["exchange"]["secret"],
            'password': self.config["exchange"].get("password", ""),
            'enableRateLimit': True,
            'proxies': self.config.get('proxy', {}),
            'options': {
                'defaultType': 'spot',
                'fetchMarkets': ['spot'],
            },
            'timeout': 30000,
            'sandbox': self.config.get('sandbox', False)  # 沙箱模式
        }
        
        self.store = CCXTStore(
            exchange=self.config['exchange']['name'],
            currency='USDT',
            config=exchange_config,
            retries=5,
            sandbox=self.config.get('sandbox', False)
        )
        self.data_manager = DataManager(self.store)
        
        try:
            markets = self.store.exchange.load_markets()
            for symbol in self.config['spot_symbols']:
                if symbol not in markets:
                    raise ValueError(f"交易对 {symbol} 不存在于现货市场！")
        except Exception as e:
            logger.error(f"交易所初始化失败: {str(e)}")
            sys.exit(1)

    def add_strategy(self):
        self.cerebro.addstrategy(
            EnhancedQueryStrategy,
            **self.config.get('monitor', {})
        )
    
    def start_visualization(self):
        if self.enable_visualization:
            self.visualizer = StrategyVisualizer(
                self.data_manager, 
                self.data_manager.db_path, 
                enabled=True
            )
            visualizer_thread = threading.Thread(
                target=self.visualizer.run_server,
                daemon=True
            )
            visualizer_thread.start()
            logger.info(f"可视化仪表板已启动: http://localhost:8050")

    def add_datafeeds(self):
        now_ts = int(time.time() * 1000)
        logger.debug(f"当前时间戳（毫秒）: {now_ts}")

        for symbol in self.config['spot_symbols']:
            valid_data = []
            try:
                timeframes = [
                    ('1h', now_ts - 365 * 86400_000, now_ts),
                    ('1m', now_ts - 30 * 86400_000, now_ts)
                ]
                
                for tf, start_ts, end_ts in timeframes:
                    logger.debug(f"计算时间范围 | 品种: {symbol} | 周期: {tf} | 起始: {start_ts} | 结束: {end_ts}")
                    # 强制对齐时间戳
                    tf_ms = self.data_manager._timeframe_to_ms(tf)
                    aligned_start = self.data_manager._align_ts(start_ts, tf_ms)
                    aligned_end = self.data_manager._align_ts(end_ts, tf_ms)
                    
                    # 确保数据范围
                    if not self.data_manager.ensure_data_range(symbol, tf):
                        logger.error(f"数据范围不满足 {symbol} {tf}")
                        continue
                    
                    # 加载数据源
                    data = self._load_datafeed(symbol, tf, start_ts, end_ts)
                    if data is not None and not data.dataname.empty:  # 严格检查有效性
                        valid_data.append(data)
                        logger.info(f"加载 {symbol} {tf} 数据 {len(data)} 条")
                    else:
                        logger.warning(f"数据加载失败: {symbol} {tf}")
                
                if valid_data:
                    for data in valid_data:
                        self.cerebro.adddata(data)
                else:
                    logger.error(f"{symbol} 无有效数据源")
                    
            except Exception as e:
                logger.error(f"数据加载异常: {symbol} - {str(e)}", exc_info=True)

    def _load_datafeed(self, symbol: str, timeframe: str, start_ts: int, end_ts: int) -> Optional[bt.feeds.PandasDirectData]:
        try:
            start_dt = datetime.fromtimestamp(start_ts/1000, tz=timezone.utc)
            end_dt = datetime.fromtimestamp(end_ts/1000, tz=timezone.utc)
            logger.debug(f"请求时间范围: {start_ts} ({datetime.fromtimestamp(start_ts/1000)}) -> {end_ts} ({datetime.fromtimestamp(end_ts/1000)})")
            data = SQLiteData(
                dbname=self.data_manager.db_path,
                symbol=symbol,
                timeframe=timeframe,
                compression=60 if timeframe == '1h' else 1,
                fromdate=start_dt,
                todate=end_dt,
                name=f"{symbol}_{timeframe}",
                sessionend=datetime(1970, 1, 1),
                tz=timezone.utc
            )
            # 计算实际需要的K线数量（MA周期 + 缓冲）
            monitor_config = self.config.get('monitor', {})
            min_bars = monitor_config.get('min_history_bars', 200)
            ma_period = monitor_config.get('ma_period', 200)
            required_min_bars = max(min_bars, ma_period) + 50
            logger.debug(f"Data length after loading: {len(data)}, required: {required_min_bars}")
            logger.debug(f"实际加载数据量: {len(data)} 条")
            return data
        except Exception as e:
            logger.error(f"数据加载失败: {symbol} {timeframe} - {str(e)}")
            return None
    
    def run_monitor_cycle(self):
        try:
            if self.config.get('historical_fill', {}).get('enabled', False):
                logger.debug("启动历史数据补全线程")
                self._safe_start_thread(self._run_historical_fill, 'HistoricalFiller')

            while not self._stop_event.is_set():
                # 主循环增加退出检查点
                if self._stop_event.is_set():
                    break
                    
                if self.config.get('gap_fill', {}).get('enabled', False):
                    logger.debug("启动缺口填充线程")
                    gap_thread = Thread(
                        target=self._run_gap_filling,
                        name='GapFiller',
                        daemon=True
                    )
                    gap_thread.start()
                    self.active_threads['GapFiller'] = gap_thread
                else:
                    logger.debug("缺口填充功能已禁用")

                # 核心数据操作增加超时机制
                try:
                    self._sync_core_data()
                    self._clean_cerebro_data()
                    self.add_datafeeds()
                    database_maintenance(self.data_manager.db_path)
                except Exception as e:
                    logger.error(f"核心数据操作异常: {str(e)}")

                # 可视化状态判断
                if not self.enable_visualization:
                    self._log_data_summary()
                
                # 策略执行增加退出检查
                self._run_strategy_with_timeout()
                self._wait_for_next_cycle()

        except KeyboardInterrupt:
            logger.info("用户终止监控")
        finally:
            self.shutdown()

    def _log_data_summary(self):
        for data in self.cerebro.datas:
            logger.info(
                f"数据源: {data._name} | 最新价格: {data.close[0]:.2f} | 时间: {data.datetime.datetime(0)}"
            )

    def _clean_cerebro_data(self):
        try:
            self.cerebro.datas.clear()
            logger.debug("已清除历史数据")
        except AttributeError as e:
            logger.warning(f"数据清除异常: {str(e)}")

    def _run_strategy_with_timeout(self, timeout=300):
        strategy_thread = threading.Thread(target=self.cerebro.run, kwargs={'runonce': False})
        strategy_thread.start()
        strategy_thread.join(timeout)

        if strategy_thread.is_alive():
            logger.error("策略执行超时，强制终止")
            self._stop_event.set()
        
    def _wait_for_next_cycle(self):
        sleep_time = 60 - datetime.now().second
        for _ in range(sleep_time):
            if self._stop_event.is_set():
                break
            time.sleep(1)
        gc.collect()

    def shutdown(self):
        if not self._stop_event.is_set():
            logger.warning("启动安全关闭流程...")
            self._stop_event.set()
            
            # 终止所有子线程
            for thread_name, thread in self.active_threads.items():
                if thread.is_alive():
                    logger.info(f"等待线程 {thread_name} 退出...")
                    thread.join(timeout=5)
                    if thread.is_alive():
                        logger.error(f"线程 {thread_name} 无法终止！")
            
            if hasattr(self.cerebro, 'stop'):
                self.cerebro.stop()
            
            if self.data_manager:
                try:
                    self.data_manager._close_db_connections()
                except Exception as e:
                    logger.error(f"关闭数据连接失败: {str(e)}")
            
            if self.visualizer:
                try:
                    self.visualizer.shutdown_flag.set()
                except Exception as e:
                    logger.error(f"停止可视化服务异常: {str(e)}")
            
            logger.info("系统资源释放完成")
    
    def safe_shutdown(self, signum, frame):
        logger.warning(f"捕获到终止信号 {signum}")
        self.shutdown()
        os._exit(0)
    
    def _validate_data_continuity(self):
        """在控制台输出数据质量"""
        for data in self.cerebro.datas:
            df = data.dataname
            expected = pd.Timedelta(minutes=len(df)*1.2)
            actual = df.index[-1] - df.index[0]
            completeness = len(df)/((actual.total_seconds()//60)+1)
            
            logger.info(
                f"数据质量: {data._name} "
                f"完整性: {completeness:.1%} "
                f"最新时间: {df.index[-1]}"
            )

    def _run_historical_fill(self):
        if not self.config.get('historical_fill', {}).get('enabled', False):
            logger.debug("历史数据补全功能已禁用")
            return
        
        now_ts = int(time.time() * 1000)
        for symbol in self.config['spot_symbols']:
            try:
                # 添加执行条件检查
                if not self.data_manager._should_process(symbol, '1h', 'historical'):
                    continue
                self._fill_history(symbol, '1h', now_ts - 365*86400*1000, now_ts)
                
                if not self.data_manager._should_process(symbol, '1m', 'historical'):
                    continue
                self._fill_history(symbol, '1m', now_ts - 30*86400*1000, now_ts)
                        
            except Exception as e:
                logger.error(f"补全流程异常 {symbol}: {str(e)}")
    
    def _fill_history(self, symbol, tf, start_ts, end_ts):
        """历史补全方法"""
        '''
        last_sync = self.data_manager._get_sync_progress(symbol, tf)
        if last_sync and (int(time.time()*1000) - last_sync < 86400_000):
            logger.info(f"⏩ 跳过近期已修复数据: {symbol} {tf}")
            return
        '''
        logger.info(f"开始补全{symbol} {tf}数据 ({datetime.fromtimestamp(start_ts/1000)} 至 {datetime.fromtimestamp(end_ts/1000)})")
        
        gap_params = self.config.get('gap_fill_params', {'max_retries':5, 'retry_delay':10})

        for attempt in range(gap_params['max_retries']):
            try:
                with self.data_manager.gap_lock:
                    # 清除旧缺口并执行补全
                    with sqlite3.connect(self.data_manager.db_path) as conn:
                        conn.execute('DELETE FROM data_gaps WHERE symbol=? AND timeframe=?', (symbol, tf))
                        conn.commit()
                    
                    # 执行数据补全
                    self.data_manager.fill_history(symbol, tf)
                    
                    # 强制二次补全确保范围
                    if not self.data_manager.ensure_data_range(symbol, tf):
                        raise ValueError("强制补全失败")
                        
                    # 标记修复状态   
                    self.data_manager._mark_as_repaired(symbol, tf)
                    break
            except Exception as e:
                logger.error(f"补全失败({attempt+1}/{gap_params['max_retries']}): {str(e)}")
                time.sleep(gap_params['retry_delay'] * (attempt+1))
            
        self.data_manager.check_and_fill_gaps(symbol, tf)

    def _run_gap_filling(self):
        if not self.config.get('gap_fill', {}).get('enabled', False):
            logger.debug("缺口填充功能已禁用")
            return
        
        with self.data_manager.gap_lock:
            for symbol in self.config['spot_symbols']:
                # 添加执行条件检查
                if not self.data_manager._should_process(symbol, '1m', 'gap'):
                    continue
                self.data_manager.check_and_fill_gaps(symbol, '1m')
                
                if not self.data_manager._should_process(symbol, '1h', 'gap'): 
                    continue
                self.data_manager.check_and_fill_gaps(symbol, '1h')
                time.sleep(5)

    def _sync_core_data(self):
        now_ts = int(time.time() * 1000)
        for symbol in self.config['spot_symbols']:
            try:
                for tf in ['1h', '1m']:
                    # 获取最新K线结束时间
                    last_ts = self.data_manager.get_last_timestamp(symbol, tf)
                    if not last_ts:
                        continue
                        
                    # 计算预期最新时间
                    tf_ms = self.data_manager._timeframe_to_ms(tf)
                    expected_end = self.data_manager._align_ts(now_ts - tf_ms, tf_ms)
                    
                    # 仅当存在缺口时才调用
                    if last_ts < expected_end:
                        logger.info(f"检测到最新数据延迟 {symbol} {tf}")
                        self.data_manager.check_and_fill_gaps(
                            symbol=symbol,
                            timeframe=tf,
                            start_ts=last_ts + tf_ms,
                            end_ts=expected_end
                        )
                        
                    # 强制检查最后3根K线的连续性
                    self.data_manager._validate_last_n_bars(symbol, tf, n=3)
                    
            except Exception as e:
                logger.error(f"同步异常: {symbol} - {str(e)}")

    def _safe_start_thread(self, target, name):
        if name in self.active_threads and self.active_threads[name].is_alive():
            return
            
        thread = threading.Thread(
            target=target,
            name=name,
            daemon=True
        )
        self.active_threads[name] = thread
        thread.start()