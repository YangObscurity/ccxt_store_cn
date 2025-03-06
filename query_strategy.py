# query_strategy.py
import logging
import os
import sys
from typing import Dict, Set, Tuple
import backtrader as bt
from backtrader import indicators as btind

current_file_path = os.path.abspath(__file__).replace('\\', '/')
root_dir = os.path.dirname(os.path.dirname(current_file_path))
sys.path.insert(0, root_dir)
from data_manager import DataManager
from datetime import datetime, timedelta
from collections import defaultdict

class EnhancedQueryStrategy(bt.Strategy):
    params = (
        ('rsi_period', 14),
        ('ma_period', 200),
        ('atr_period', 14),
        ('min_history_bars', 200),
        ('oversold', 20),
        ('overbought', 90),
        ('debug', False),
        ('enable_plot', True)
    )

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.inds = {}
        self.plot_data = defaultdict(lambda: {
            'time': [], 'price': [], 'ma': [], 'rsi': [], 'atr': [], 'signals': []
        })
        self.signals: Set[Tuple[str, int]] = set()

        for data in self.datas:
            if not hasattr(data, '_name') or not data._name:
                continue
            if len(data.close) < self.params.ma_period:
                continue
            
            try:
                self.inds[data] = {
                    'ma': btind.SMA(data.close, period=self.params.ma_period),
                    'rsi': btind.RSI(data.close, period=self.params.rsi_period),
                    'atr': btind.ATR(data, period=self.params.atr_period)
                }
            except Exception as e:
                continue

    def next(self):
        for data, ind in self.inds.items():
            if any([v[0] is None for v in ind.values()]):
                continue
            
            try:
                condition = (
                    (ind['rsi'][0] < self.params.oversold) and
                    (data.close[0] > ind['ma'][0]) and
                    (data.close[0] > data.close[-1] + ind['atr'][0])
                )
                if condition:
                    self._log_signal(data, ind)
                self._record_data(data)
            except IndexError:
                continue
            
    def _record_data(self, data):
        try:
            if data._name not in self.inds:
                return
                
            ind = self.inds[data]
            if any([v[0] is None for v in ind.values()]):
                return

            plot_time = int(data.datetime.datetime().timestamp() * 1000)
            self.plot_data[data._name]['time'].append(plot_time)
            self.plot_data[data._name]['price'].append(float(data.close[0]))
            self.plot_data[data._name]['ma'].append(float(ind['ma'][0]))
            self.plot_data[data._name]['rsi'].append(float(ind['rsi'][0]))
            self.plot_data[data._name]['atr'].append(float(ind['atr'][0]))
            self.plot_data[data._name]['signals'].append(None)  # 占位符
        except (KeyError, IndexError, AttributeError) as e:
            self.logger.warning(f"数据记录失败: {str(e)}")

    def _log_signal(self, data, ind):
        plot_time = self.plot_data[data._name]['time'][-1]
        self.signals.add((data._name.split('_')[0], plot_time))
        
        # 更新信号标记
        last_idx = len(self.plot_data[data._name]['signals']) - 1
        self.plot_data[data._name]['signals'][last_idx] = 'BUY'

        log_msg = (
            f"{data.datetime.datetime()} | {data._name} | BUY | "
            f"Price: {data.close[0]:.2f} | "
            f"RSI: {ind['rsi'][0]:.2f} | "
            f"MA: {ind['ma'][0]:.2f} | "
            f"ATR: {ind['atr'][0]:.2f}"
        )
        self.logger.info(log_msg)


    def stop(self):
        if not self.params.enable_plot:
            return
            
        dm = DataManager()
        for symbol, data in self.plot_data.items():
            if not data['time']:
                continue
                
            try:
                clean_data = {
                    'time': data['time'],
                    'price': data['price'],
                    'ma': data['ma'],
                    'rsi': data['rsi'],
                    'atr': data['atr'],
                    'signals': [
                        'BUY' if (symbol.split('_')[0], t) in self.signals else None 
                        for t in data['time']
                    ]
                }
                # 严格验证数据长度一致性
                if not all(len(lst) == len(data['time']) for lst in [data['price'], data['ma'], data['rsi'], data['atr']]):
                    raise ValueError("指标数据长度不一致")
                    
                dm.save_plot_data(
                    symbol=symbol.split('_')[0],
                    timeframe='1h' if '1h' in symbol else '1m',
                    data=clean_data
                )
            except Exception as e:
                self.logger.error(f"DATA SAVE FAILED [{symbol}]: {str(e)}")